use crate::internal::common::resources::request::{
    AllocationRequest, ResourceRequest, ResourceRequestEntry, ResourceRequestVariants,
};
use crate::internal::common::resources::{ResourceId, ResourceVec};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::internal::worker::resources::pool::ResourcePool;
use crate::resources::{
    Allocation, ResourceAmount, ResourceDescriptor, ResourceMap, ResourceUnits,
};
use std::rc::Rc;
use std::time::Duration;

pub struct ResourceAllocator {
    pools: ResourceVec<ResourcePool>,
    free_resources: ConciseFreeResources,
    remaining_time: Option<Duration>,

    blocked_requests: Vec<BlockedRequest>,
    higher_priority_blocked_requests: usize,
    running_tasks: Vec<Rc<Allocation>>, // TODO: Rework on multiset?
    own_resources: WorkerResources,
}

/// Resource request that cannot be scheduled in the current free resources
#[derive(Debug)]
struct BlockedRequest {
    request: ResourceRequest,
    /// Reachable state of free resources AFTER some of currently running tasks is finished AND
    /// this resource request is enabled
    witnesses: Vec<ConciseFreeResources>,
}

impl ResourceAllocator {
    pub fn new(
        desc: &ResourceDescriptor,
        resource_map: &ResourceMap,
        label_map: &ResourceLabelMap,
    ) -> Self {
        let max_id = desc
            .resources
            .iter()
            .map(|item| {
                resource_map
                    .get_index(&item.name)
                    .expect("Internal error, unknown resource name")
            })
            .max()
            .expect("Allocator needs at least one resource");
        let r_counts = max_id.as_num() as usize + 1;
        let mut pools = ResourceVec::default();
        pools.resize_with(r_counts, || ResourcePool::Empty);

        for item in &desc.resources {
            let idx = resource_map.get_index(&item.name).unwrap();
            pools[idx] = ResourcePool::new(&item.kind, idx, label_map);
        }

        let free_resources = ConciseFreeResources::new(
            pools
                .iter()
                .map(|p| p.concise_state())
                .collect::<Vec<_>>()
                .into(),
        );

        ResourceAllocator {
            pools,
            free_resources,
            remaining_time: None,
            blocked_requests: Vec::new(),
            higher_priority_blocked_requests: 0,
            running_tasks: Vec::new(),
            own_resources: WorkerResources::from_description(desc, resource_map),
        }
    }

    pub fn is_capable_to_run(&self, request: &ResourceRequest) -> bool {
        self.own_resources.is_capable_to_run_request(request)
    }

    pub fn reset_temporaries(&mut self, remaining_time: Option<Duration>) {
        self.remaining_time = remaining_time;
        self.higher_priority_blocked_requests = 0;
        self.blocked_requests.clear();
    }

    fn release_allocation_helper(&mut self, allocation: &Allocation) {
        for al in &allocation.resources {
            self.pools[al.resource_id].release_allocation(al);
        }
    }

    pub fn release_allocation(&mut self, allocation: Rc<Allocation>) {
        self.free_resources.add(&allocation);
        self.release_allocation_helper(&allocation);
        let position = self
            .running_tasks
            .iter()
            .position(|a| Rc::ptr_eq(a, &allocation))
            .unwrap();
        self.running_tasks.swap_remove(position);
    }

    pub fn take_running_allocations(self) -> Vec<Rc<Allocation>> {
        self.running_tasks
    }

    pub fn close_priority_level(&mut self) {
        self.higher_priority_blocked_requests = self.blocked_requests.len();
    }

    fn has_resources_for_entry(
        pool: &ResourcePool,
        free: &ConciseResourceState,
        policy: &AllocationRequest,
    ) -> bool {
        let max_alloc = free.amount_max_alloc();
        match policy {
            AllocationRequest::Compact(amount) | AllocationRequest::Scatter(amount) => {
                *amount <= max_alloc
            }
            AllocationRequest::ForceCompact(amount) => {
                if amount.is_zero() {
                    return true;
                }
                if *amount > max_alloc {
                    return false;
                }
                let (units, fractions) = amount.split();
                if fractions > 0 {
                    // Fractions and ForceCompact is not supported in the current version
                    // It is checked in resource request validation, this code should be unreachable
                    unreachable!()
                }
                let socket_count = (((units - 1) / pool.min_group_size()) as usize) + 1;
                if free.n_groups() < socket_count {
                    return false;
                }
                let mut groups: Vec<ResourceUnits> = free.groups().to_vec();
                groups.sort_unstable();
                let sum: ResourceUnits = groups.iter().rev().take(socket_count).sum();

                units <= sum
            }
            AllocationRequest::All => max_alloc == pool.full_size(),
        }
    }

    fn compute_witness<'b>(
        pools: &[ResourcePool],
        free: &ConciseFreeResources,
        request: &ResourceRequest,
        running: impl Iterator<Item = &'b Rc<Allocation>>,
    ) -> Option<ConciseFreeResources> {
        let mut free = free.clone();
        if Self::has_resources_for_request(pools, &free, request) {
            return Some(free);
        }
        for running_request in running {
            free.add(running_request);
            if Self::has_resources_for_request(pools, &free, request) {
                return Some(free);
            }
        }
        None
    }

    fn has_resources_for_request(
        pools: &[ResourcePool],
        free: &ConciseFreeResources,
        request: &ResourceRequest,
    ) -> bool {
        request.entries().iter().all(|entry| {
            pools
                .get(entry.resource_id.as_num() as usize)
                .map(|pool| {
                    Self::has_resources_for_entry(pool, free.get(entry.resource_id), &entry.request)
                })
                .unwrap_or(false)
        })
    }

    fn compute_witnesses(
        pools: &[ResourcePool],
        free_resources: &ConciseFreeResources,
        running: &mut [Rc<Allocation>],
        request: &ResourceRequest,
    ) -> Vec<ConciseFreeResources> {
        let mut witnesses: Vec<ConciseFreeResources> =
            Vec::with_capacity(2 * free_resources.n_resources());

        let compute_witnesses = |witnesses: &mut Vec<ConciseFreeResources>,
                                 running: &[Rc<Allocation>]| {
            let w = Self::compute_witness(pools, free_resources, request, running.iter());
            witnesses.push(w.unwrap());
            let w = Self::compute_witness(pools, free_resources, request, running.iter().rev());
            witnesses.push(w.unwrap());
        };

        for i in 0..free_resources.n_resources() {
            let idx = ResourceId::from(i as u32);
            running.sort_unstable_by_key(|x| {
                x.resource_allocation(idx)
                    .map(|a| a.amount)
                    .unwrap_or(ResourceAmount::ZERO)
            });
            compute_witnesses(&mut witnesses, running);
        }
        witnesses
    }

    fn new_blocked_request(&mut self, request: &ResourceRequest) {
        self.blocked_requests.push(BlockedRequest {
            request: request.clone(),
            witnesses: Vec::new(),
        });
    }

    fn check_blocked_request(&mut self, allocation: &Allocation) -> bool {
        for blocked in &mut self.blocked_requests[..self.higher_priority_blocked_requests] {
            if blocked.witnesses.is_empty() {
                blocked.witnesses = Self::compute_witnesses(
                    &self.pools,
                    &self.free_resources,
                    self.running_tasks.as_mut_slice(),
                    &blocked.request,
                )
            }
            for witness in &blocked.witnesses {
                let mut free = witness.clone();
                free.remove(allocation);
                if !Self::has_resources_for_request(&self.pools, &free, &blocked.request) {
                    return false;
                }
            }
        }
        true
    }

    pub fn try_allocate(
        &mut self,
        request: &ResourceRequestVariants,
    ) -> Option<(Rc<Allocation>, usize)> {
        request
            .requests()
            .iter()
            .enumerate()
            .find_map(|(i, r)| self.try_allocate_variant(r).map(|c| (c, i)))
    }

    fn try_allocate_variant(&mut self, request: &ResourceRequest) -> Option<Rc<Allocation>> {
        if let Some(remaining_time) = self.remaining_time {
            if remaining_time < request.min_time() {
                return None;
            }
        }

        if self.blocked_requests.iter().any(|b| &b.request == request) {
            return None;
        }

        if !Self::has_resources_for_request(&self.pools, &self.free_resources, request) {
            self.new_blocked_request(request);
            return None;
        }

        let mut allocation = Allocation::new();
        for entry in request.entries() {
            let pool = self.pools.get_mut(entry.resource_id.as_usize()).unwrap();
            allocation
                .add_resource_allocation(pool.claim_resources(entry.resource_id, &entry.request))
        }
        if !self.check_blocked_request(&allocation) {
            self.release_allocation_helper(&allocation);
            return None;
        }
        self.free_resources.remove(&allocation);
        let allocation_rc = Rc::new(allocation);
        self.running_tasks.push(allocation_rc.clone());
        Some(allocation_rc)
    }

    pub fn difficulty_score(&self, entry: &ResourceRequestEntry) -> f32 {
        let size = self
            .pools
            .get(entry.resource_id)
            .map(|x| x.full_size())
            .unwrap_or(ResourceAmount::ZERO);
        if size.is_zero() {
            0.0f32
        } else {
            match entry.request {
                AllocationRequest::Compact(amount) | AllocationRequest::Scatter(amount) => {
                    amount.total_fractions() as f32 / size.total_fractions() as f32
                }
                AllocationRequest::ForceCompact(amount)
                    if self.pools[entry.resource_id].n_groups() == 1 =>
                {
                    amount.total_fractions() as f32 / size.total_fractions() as f32
                }
                AllocationRequest::ForceCompact(amount) => {
                    (amount.total_fractions() * 2) as f32 / size.total_fractions() as f32
                }
                AllocationRequest::All => 2.0,
            }
        }
    }

    pub fn validate(&self) {
        #[cfg(debug_assertions)]
        for (pool, state) in self.pools.iter().zip(self.free_resources.all_states()) {
            pool.validate();
            assert_eq!(pool.concise_state().strip_zeros(), state.strip_zeros());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::descriptor::{
        ResourceDescriptor, ResourceDescriptorKind,
    };
    use crate::internal::common::resources::{Allocation, ResourceId, ResourceRequestVariants};
    use crate::internal::tests::utils::resources::{ResBuilder, cpus_compact};
    use crate::internal::tests::utils::shared::res_allocator_from_descriptor;
    use crate::internal::tests::utils::sorted_vec;
    use crate::internal::worker::resources::allocator::ResourceAllocator;
    use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
    use crate::internal::worker::resources::pool::ResourcePool;
    use crate::resources::{ResourceAmount, ResourceDescriptorItem, ResourceIndex, ResourceUnits};

    use std::rc::Rc;
    use std::time::Duration;

    impl ResourceAllocator {
        pub fn get_sockets(&self, allocation: &Allocation, idx: u32) -> Vec<usize> {
            let idx = ResourceId::new(idx);
            let al = allocation.resource_allocation(idx).unwrap();
            self.pools[idx].get_sockets(al)
        }

        pub fn get_current_free(&self, idx: u32) -> ResourceAmount {
            self.pools[idx.into()].current_free()
        }
    }

    pub fn simple_descriptor(
        n_sockets: ResourceUnits,
        socket_size: ResourceUnits,
    ) -> ResourceDescriptor {
        ResourceDescriptor::new(vec![ResourceDescriptorItem {
            name: "cpus".to_string(),
            kind: ResourceDescriptorKind::regular_sockets(n_sockets, socket_size),
        }])
    }

    pub fn test_allocator(descriptor: &ResourceDescriptor) -> ResourceAllocator {
        res_allocator_from_descriptor(descriptor.clone())
    }

    fn simple_allocator(
        free: &[ResourceUnits],
        running: &[&[ResourceUnits]],
        remaining_time: Option<Duration>,
    ) -> ResourceAllocator {
        let mut names = vec!["cpus".to_string()];
        names.extend((1..free.len()).map(|i| format!("res{i}")));
        let ds: Vec<_> = free
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let mut total = *c;
                for r in running {
                    if i < r.len() {
                        total += r[i]
                    }
                }
                ResourceDescriptorItem {
                    name: names[i].clone(),
                    kind: ResourceDescriptorKind::simple_indices(total),
                }
            })
            .collect();
        let descriptor = ResourceDescriptor::new(ds);

        let mut ac = res_allocator_from_descriptor(descriptor);
        for r in running {
            assert_eq!(r.len(), 1); // As far I see, only 1 element array is now used in tests
            let units = r[0];
            let rq = ResBuilder::default()
                .add(0, ResourceAmount::new_units(units))
                .finish();
            ac.try_allocate_variant(&rq).unwrap();
        }
        ac.reset_temporaries(remaining_time);
        ac
    }

    fn simple_alloc(
        allocator: &mut ResourceAllocator,
        counts: &[ResourceUnits],
        expect_pass: bool,
    ) {
        let mut builder = ResBuilder::default();
        for (i, count) in counts.iter().enumerate() {
            if *count > 0 {
                builder = builder.add(
                    ResourceId::from(i as u32),
                    ResourceAmount::new_units(*count),
                );
            }
        }
        let al = allocator.try_allocate_variant(&builder.finish());
        if expect_pass {
            al.unwrap();
        } else {
            assert!(al.is_none());
        }
    }

    #[test]
    fn test_compute_blocking_level() {
        let pools = [
            ResourcePool::new(
                &ResourceDescriptorKind::simple_indices(3),
                0.into(),
                &Default::default(),
            ),
            ResourcePool::new(
                &ResourceDescriptorKind::simple_indices(3),
                0.into(),
                &Default::default(),
            ),
        ];
        let free = ConciseFreeResources::new_simple(&[1, 2]);
        let rq = ResBuilder::default().add(0, 3).add(1, 1).finish();

        assert!(ResourceAllocator::compute_witness(&pools, &free, &rq, [].iter()).is_none());
        assert!(
            ResourceAllocator::compute_witness(
                &pools,
                &free,
                &rq,
                [Rc::new(Allocation::new_simple(&[1]))].iter()
            )
            .is_none()
        );
        assert_eq!(
            ResourceAllocator::compute_witness(
                &pools,
                &free,
                &rq,
                [
                    Rc::new(Allocation::new_simple(&[1])),
                    Rc::new(Allocation::new_simple(&[1])),
                    Rc::new(Allocation::new_simple(&[1]))
                ]
                .iter()
            ),
            Some(ConciseFreeResources::new_simple(&[3, 2]))
        );

        let free = ConciseFreeResources::new_simple(&[4, 2]);
        assert_eq!(
            ResourceAllocator::compute_witness(&pools, &free, &rq, [].iter()),
            Some(free.clone())
        );

        let rq = ResBuilder::default().add(0, 4).add(1, 4).finish();
        assert_eq!(
            ResourceAllocator::compute_witness(
                &pools,
                &free,
                &rq,
                [
                    Rc::new(Allocation::new_simple(&[2])),
                    Rc::new(Allocation::new_simple(&[2])),
                    Rc::new(Allocation::new_simple(&[2])),
                    Rc::new(Allocation::new_simple(&[0, 3])),
                    Rc::new(Allocation::new_simple(&[0, 3]))
                ]
                .iter()
            ),
            Some(ConciseFreeResources::new_simple(&[10, 5]))
        );
    }

    #[test]
    fn test_allocator_single_socket() {
        let mut allocator = simple_allocator(&[4], &[], None);

        simple_alloc(&mut allocator, &[3], true);
        allocator.free_resources.assert_eq(&[1]);

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[1]);

        simple_alloc(&mut allocator, &[1], true);
        allocator.free_resources.assert_eq(&[0]);

        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check1() {
        // Running: [1,1,1], free: [1], try: [2p1] [1p0]
        let mut allocator = simple_allocator(&[1], &[&[1], &[1], &[1]], None);
        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[1]);
        allocator.validate();

        allocator.close_priority_level();

        simple_alloc(&mut allocator, &[1], false);
        allocator.free_resources.assert_eq(&[1]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check2() {
        // Running: [1,1,1], free: [1], try: [2p0] [1p0]
        let mut allocator = simple_allocator(&[1], &[&[1], &[1], &[1]], None);

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[1]);

        simple_alloc(&mut allocator, &[1], true);
        allocator.free_resources.assert_eq(&[0]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check3() {
        // Running: [3], free: [1], try: [2p1] [1p0]
        let mut allocator = simple_allocator(&[1], &[&[3]], None);

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[1]);

        allocator.close_priority_level();

        simple_alloc(&mut allocator, &[1], true);
        allocator.free_resources.assert_eq(&[0]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check4() {
        // Running: [3], free: [1], try: [2p0] [1p0]
        let mut allocator = simple_allocator(&[1], &[&[3]], None);

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[1]);

        simple_alloc(&mut allocator, &[1], true);
        allocator.free_resources.assert_eq(&[0]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check5() {
        // Running: [], free: [4], try: [3p2] [2p1] [1p0]
        let mut allocator = simple_allocator(&[4], &[], None);

        simple_alloc(&mut allocator, &[3], true);
        allocator.free_resources.assert_eq(&[1]);

        allocator.close_priority_level();

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[1]);

        allocator.close_priority_level();

        simple_alloc(&mut allocator, &[1], true);
        allocator.free_resources.assert_eq(&[0]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check6() {
        let mut allocator = simple_allocator(&[10], &[], None);

        for _ in 0..5 {
            simple_alloc(&mut allocator, &[2], true);
        }
        allocator.free_resources.assert_eq(&[0]);

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[0]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check7() {
        let mut allocator = simple_allocator(&[10], &[], None);

        for _ in 0..5 {
            simple_alloc(&mut allocator, &[2], true);
            allocator.close_priority_level();
        }
        allocator.free_resources.assert_eq(&[0]);
        allocator.close_priority_level();

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[0]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check8() {
        let mut allocator = simple_allocator(&[1], &[&[1], &[1], &[1], &[2]], None);

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[1]);

        allocator.close_priority_level();

        simple_alloc(&mut allocator, &[1], false);
        allocator.free_resources.assert_eq(&[1]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check9() {
        let mut allocator = simple_allocator(&[1], &[&[2], &[1], &[1], &[1]], None);

        simple_alloc(&mut allocator, &[2], false);
        allocator.free_resources.assert_eq(&[1]);

        allocator.close_priority_level();

        simple_alloc(&mut allocator, &[1], false);
        allocator.free_resources.assert_eq(&[1]);
        allocator.validate();
    }

    #[test]
    fn test_allocator_priority_check10() {
        let mut allocator = simple_allocator(&[1], &[&[3], &[1], &[3], &[1]], None);

        simple_alloc(&mut allocator, &[3], false);
        allocator.free_resources.assert_eq(&[1]);

        allocator.close_priority_level();

        simple_alloc(&mut allocator, &[1], false);
        allocator.free_resources.assert_eq(&[1]);
        allocator.validate();
    }

    #[test]
    fn test_pool_single_socket() {
        let descriptor = simple_descriptor(1, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq = cpus_compact(3).finish_v();
        let (al, idx) = allocator.try_allocate(&rq).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(al.resources.len(), 1);
        assert_eq!(al.resources[0].resource_id, ResourceId::new(0));
        assert_eq!(al.resources[0].indices.len(), 3);
        assert!(al.resources[0].resource_indices().all(|x| x.as_num() < 4));

        let rq = cpus_compact(2).finish_v();
        assert!(allocator.try_allocate(&rq).is_none());

        assert_eq!(allocator.running_tasks.len(), 1);
        allocator.release_allocation(al);
        assert_eq!(allocator.running_tasks.len(), 0);

        allocator.reset_temporaries(None);

        let rq = cpus_compact(4).finish_v();
        let (al, _idx) = allocator.try_allocate(&rq).unwrap();
        assert_eq!(
            al.resources[0].resource_indices().collect::<Vec<_>>(),
            vec![3.into(), 2.into(), 1.into(), 0.into()]
        );
        assert_eq!(allocator.running_tasks.len(), 1);
        allocator.release_allocation(al);
        assert_eq!(allocator.running_tasks.len(), 0);
        allocator
            .free_resources
            .get(0.into())
            .amount_sum()
            .assert_eq_units(4);
        assert_eq!(allocator.free_resources.get(0.into()).n_groups(), 1);

        allocator.reset_temporaries(None);

        let rq = cpus_compact(1).finish_v();
        let rq2 = cpus_compact(2).finish_v();
        let (al1, _) = allocator.try_allocate(&rq).unwrap();
        let (al2, _) = allocator.try_allocate(&rq).unwrap();
        let (al3, _) = allocator.try_allocate(&rq).unwrap();
        let (al4, _) = allocator.try_allocate(&rq).unwrap();
        assert!(allocator.try_allocate(&rq).is_none());
        assert!(allocator.try_allocate(&rq2).is_none());

        allocator.release_allocation(al2);
        allocator.release_allocation(al4);

        allocator.reset_temporaries(None);

        let (al5, _) = allocator.try_allocate(&rq2).unwrap();

        assert!(allocator.try_allocate(&rq).is_none());
        assert!(allocator.try_allocate(&rq2).is_none());

        let mut v = Vec::new();
        v.extend(al1.resources[0].resource_indices());
        assert_eq!(v.len(), 1);
        v.extend(al5.resources[0].resource_indices());
        assert_eq!(v.len(), 3);
        v.extend(al3.resources[0].resource_indices());
        assert_eq!(sorted_vec(v), vec![0.into(), 1.into(), 2.into(), 3.into()]);
        allocator.validate();
    }

    #[test]
    fn test_pool_compact1() {
        let descriptor = simple_descriptor(4, 6);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = cpus_compact(4).finish_v();
        let (al1, _) = allocator.try_allocate(&rq1).unwrap();
        let s1 = allocator.get_sockets(&al1, 0);
        assert_eq!(s1.len(), 1);
        let (al2, _) = allocator.try_allocate(&rq1).unwrap();
        let s2 = allocator.get_sockets(&al2, 0);
        assert_eq!(s2.len(), 1);
        assert_ne!(s1, s2);

        let rq2 = cpus_compact(3).finish_v();
        let (al3, _) = allocator.try_allocate(&rq2).unwrap();
        let s3 = allocator.get_sockets(&al3, 0);
        assert_eq!(s3.len(), 1);
        let (al4, _) = allocator.try_allocate(&rq2).unwrap();
        let s4 = allocator.get_sockets(&al4, 0);
        assert_eq!(s4.len(), 1);
        assert_ne!(s3, s1);
        assert_ne!(s4, s1);
        assert_ne!(s3, s2);
        assert_ne!(s4, s2);
        assert_eq!(s3, s4);

        allocator.close_priority_level();

        let rq3 = cpus_compact(6).finish_v();
        let (al, _) = allocator.try_allocate(&rq3).unwrap();
        assert_eq!(allocator.get_sockets(&al, 0).len(), 1);
        allocator.release_allocation(al);
        allocator.reset_temporaries(None);

        let rq3 = cpus_compact(7).finish_v();
        let (al, _) = allocator.try_allocate(&rq3).unwrap();
        assert_eq!(allocator.get_sockets(&al, 0).len(), 2);

        allocator.release_allocation(al);
        allocator.reset_temporaries(None);

        let rq3 = cpus_compact(8).finish_v();
        let (al, _) = allocator.try_allocate(&rq3).unwrap();
        assert_eq!(allocator.get_sockets(&al, 0).len(), 2);
        allocator.release_allocation(al);
        allocator.reset_temporaries(None);

        let rq3 = cpus_compact(9).finish_v();
        let (al, _) = allocator.try_allocate(&rq3).unwrap();
        assert_eq!(allocator.get_sockets(&al, 0).len(), 3);
        allocator.release_allocation(al);
        allocator.validate();
    }

    #[test]
    fn test_pool_allocate_compact_all() {
        let descriptor = simple_descriptor(4, 6);
        let mut allocator = test_allocator(&descriptor);

        let rq = cpus_compact(24).finish_v();
        let (al, _) = allocator.try_allocate(&rq).unwrap();
        assert_eq!(
            al.resource_allocation(0.into())
                .unwrap()
                .resource_indices()
                .collect::<Vec<_>>(),
            (0..24u32).rev().map(ResourceIndex::new).collect::<Vec<_>>()
        );
        assert!(allocator.get_current_free(0).is_zero());
        allocator.release_allocation(al);
        assert_eq!(allocator.get_current_free(0), ResourceAmount::new_units(24));
        allocator.validate();
    }

    #[test]
    fn test_pool_allocate_all() {
        let descriptor = simple_descriptor(4, 6);
        let mut allocator = test_allocator(&descriptor);

        let rq = ResBuilder::default().add_all(0).finish_v();
        let (al, _) = allocator.try_allocate(&rq).unwrap();
        assert_eq!(
            al.resource_allocation(0.into())
                .unwrap()
                .resource_indices()
                .collect::<Vec<_>>(),
            (0..24u32).map(ResourceIndex::new).collect::<Vec<_>>()
        );
        assert!(allocator.get_current_free(0).is_zero());
        allocator.release_allocation(al);
        assert_eq!(allocator.get_current_free(0), ResourceAmount::new_units(24));

        allocator.reset_temporaries(None);

        let rq2 = cpus_compact(1).finish_v();
        assert!(allocator.try_allocate(&rq2).is_some());
        assert!(allocator.try_allocate(&rq).is_none());
        allocator.validate();
    }

    #[test]
    fn test_pool_force_compact1() {
        let descriptor = simple_descriptor(2, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_force_compact(0, 9).finish_v();
        assert!(allocator.try_allocate(&rq1).is_none());

        let rq2 = ResBuilder::default().add_force_compact(0, 2).finish_v();
        for _ in 0..4 {
            let (al1, _) = allocator.try_allocate(&rq2).unwrap();
            assert_eq!(al1.get_indices(0).len(), 2);
            assert_eq!(allocator.get_sockets(&al1, 0).len(), 1);
        }

        assert!(allocator.try_allocate(&rq2).is_none());
        allocator.validate();
    }

    #[test]
    fn test_pool_force_compact2() {
        let descriptor = simple_descriptor(2, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_force_compact(0, 3).finish_v();
        for _ in 0..2 {
            let (al1, _) = allocator.try_allocate(&rq1).unwrap();
            assert_eq!(al1.get_indices(0).len(), 3);
            assert_eq!(allocator.get_sockets(&al1, 0).len(), 1);
        }

        let rq1 = ResBuilder::default().add_force_compact(0, 2).finish_v();
        assert!(allocator.try_allocate(&rq1).is_none());

        let rq1 = cpus_compact(2).finish_v();
        assert!(allocator.try_allocate(&rq1).is_some());
        allocator.validate();
    }

    #[test]
    fn test_pool_force_compact3() {
        let descriptor = simple_descriptor(3, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_force_compact(0, 8).finish_v();
        let (al1, _) = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 8);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);
        allocator.release_allocation(al1);
        allocator.validate();

        allocator.reset_temporaries(None);

        let rq1 = ResBuilder::default().add_force_compact(0, 5).finish_v();
        let (al1, _) = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 5);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);
        allocator.release_allocation(al1);
        allocator.validate();

        allocator.reset_temporaries(None);

        let rq1 = ResBuilder::default().add_force_compact(0, 10).finish_v();
        let (al1, _idx) = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 10);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);
        allocator.release_allocation(al1);
        allocator.validate();
    }

    #[test]
    fn test_pool_force_scatter1() {
        let descriptor = simple_descriptor(3, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_scatter(0, 3).finish_v();
        let (al1, _) = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 3);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);

        let rq1 = ResBuilder::default().add_scatter(0, 4).finish_v();
        let (al1, _) = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 4);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);

        let rq1 = ResBuilder::default().add_scatter(0, 2).finish_v();
        let (al1, _) = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 2);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);

        allocator.validate();
    }

    #[test]
    fn test_pool_force_scatter2() {
        let descriptor = simple_descriptor(3, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_force_compact(0, 4).finish_v();
        let _al1 = allocator.try_allocate(&rq1).unwrap();

        let rq2 = ResBuilder::default().add_scatter(0, 5).finish_v();
        let (al2, _) = allocator.try_allocate(&rq2).unwrap();
        assert_eq!(al2.get_indices(0).len(), 5);
        assert_eq!(allocator.get_sockets(&al2, 0).len(), 2);

        allocator.validate();
    }

    #[test]
    fn test_pool_generic_resources() {
        let descriptor = ResourceDescriptor::new(vec![
            ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(1, 4),
            },
            ResourceDescriptorItem {
                name: "res0".to_string(),
                kind: ResourceDescriptorKind::Range {
                    start: 5.into(),
                    end: 100.into(),
                },
            },
            ResourceDescriptorItem {
                name: "res1".to_string(),
                kind: ResourceDescriptorKind::Sum {
                    size: ResourceAmount::new_units(100_000_000),
                },
            },
            ResourceDescriptorItem {
                name: "res2".to_string(),
                kind: ResourceDescriptorKind::simple_indices(2),
            },
            ResourceDescriptorItem {
                name: "res3".to_string(),
                kind: ResourceDescriptorKind::simple_indices(2),
            },
        ]);
        let mut allocator = test_allocator(&descriptor);

        assert_eq!(
            allocator.free_resources,
            ConciseFreeResources::new(
                vec![
                    ConciseResourceState::new_simple(&[4]),
                    ConciseResourceState::new_simple(&[96]),
                    ConciseResourceState::new_simple(&[100_000_000]),
                    ConciseResourceState::new_simple(&[2]),
                    ConciseResourceState::new_simple(&[2]),
                ]
                .into()
            )
        );

        let rq: ResourceRequestVariants = cpus_compact(1)
            .add(4, 1)
            .add(1, 12)
            .add(2, 1_000_000)
            .finish_v();
        rq.validate().unwrap();
        let (al, _) = allocator.try_allocate(&rq).unwrap();
        assert_eq!(al.resources.len(), 4);
        assert_eq!(al.resources[0].resource_id.as_num(), 0);

        assert_eq!(al.resources[1].resource_id.as_num(), 1);
        assert_eq!(al.resources[1].indices.len(), 12);

        assert_eq!(al.resources[2].resource_id.as_num(), 2);
        assert_eq!(al.resources[2].amount, ResourceAmount::new_units(1_000_000));

        assert_eq!(al.resources[3].resource_id.as_num(), 4);
        assert_eq!(al.resources[3].indices.len(), 1);

        assert_eq!(allocator.get_current_free(1), ResourceAmount::new_units(84));
        assert_eq!(
            allocator.get_current_free(2),
            ResourceAmount::new_units(99_000_000)
        );
        assert_eq!(allocator.get_current_free(3), ResourceAmount::new_units(2));
        assert_eq!(allocator.get_current_free(4), ResourceAmount::new_units(1));

        let rq = cpus_compact(1).add(4, 2).finish_v();
        assert!(allocator.try_allocate(&rq).is_none());

        allocator.release_allocation(al);

        assert_eq!(allocator.get_current_free(1), ResourceAmount::new_units(96));
        assert_eq!(
            allocator.get_current_free(2),
            ResourceAmount::new_units(100_000_000)
        );
        assert_eq!(allocator.get_current_free(3), ResourceAmount::new_units(2));
        assert_eq!(allocator.get_current_free(4), ResourceAmount::new_units(2));

        allocator.reset_temporaries(None);
        assert!(allocator.try_allocate(&rq).is_some());

        allocator.validate();
    }

    #[test]
    fn test_allocator_remaining_time_no_known() {
        let descriptor = simple_descriptor(1, 4);
        let mut allocator = test_allocator(&descriptor);

        allocator.reset_temporaries(None);
        let rq = ResBuilder::default()
            .add(0, 1)
            .min_time_secs(100)
            .finish_v();
        let (al, _) = allocator.try_allocate(&rq).unwrap();
        allocator.release_allocation(al);

        allocator.reset_temporaries(Some(Duration::from_secs(101)));
        let (al, _) = allocator.try_allocate(&rq).unwrap();
        allocator.release_allocation(al);

        allocator.reset_temporaries(Some(Duration::from_secs(99)));
        assert!(allocator.try_allocate(&rq).is_none());

        allocator.validate();
    }

    #[test]
    fn test_allocator_sum_max_fractions() {
        let descriptor = ResourceDescriptor::new(vec![ResourceDescriptorItem {
            name: "cpus".to_string(),
            kind: ResourceDescriptorKind::Sum {
                size: ResourceAmount::new(0, 300),
            },
        }]);
        let mut allocator = test_allocator(&descriptor);
        allocator.reset_temporaries(None);
        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(1, 0))
            .finish_v();
        assert!(allocator.try_allocate(&rq).is_none());
        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(0, 301))
            .finish_v();
        assert!(allocator.try_allocate(&rq).is_none());
        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(0, 250))
            .finish_v();
        assert!(allocator.try_allocate(&rq).is_some());
    }

    #[test]
    fn test_allocator_indices_and_fractions() {
        let descriptor = simple_descriptor(1, 4);
        let mut allocator = test_allocator(&descriptor);
        allocator.reset_temporaries(None);
        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(4, 1))
            .finish_v();
        assert!(allocator.try_allocate(&rq).is_none());

        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(2, 1500))
            .finish_v();
        let al1 = allocator.try_allocate(&rq).unwrap().0;
        assert_eq!(al1.resources[0].indices.len(), 3);
        assert_eq!(al1.resources[0].indices[0].fractions, 0);
        assert_eq!(al1.resources[0].indices[1].fractions, 0);
        assert_eq!(al1.resources[0].indices[2].fractions, 1500);
        assert_eq!(al1.resources[0].amount, ResourceAmount::new(2, 1500));

        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(0, 5200))
            .finish_v();
        let al2 = allocator.try_allocate(&rq).unwrap().0;
        assert_eq!(al2.resources[0].indices.len(), 1);
        assert_eq!(al2.resources[0].indices[0].fractions, 5200);
        assert_eq!(
            al2.resources[0].indices[0].index,
            al1.resources[0].indices[2].index
        );
        assert_eq!(al2.resources[0].amount, ResourceAmount::new(0, 5200));

        let al3 = allocator.try_allocate(&rq).unwrap().0;
        assert_eq!(al3.resources[0].indices.len(), 1);
        assert_eq!(al3.resources[0].indices[0].fractions, 5200);
        assert_ne!(
            al3.resources[0].indices[0].index,
            al1.resources[0].indices[2].index
        );
        assert_eq!(al3.resources[0].amount, ResourceAmount::new(0, 5200));

        assert!(allocator.try_allocate(&rq).is_none());

        allocator.release_allocation(al1);

        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new(2, 9600)
        );

        allocator.release_allocation(al3);
        allocator.release_allocation(al2);

        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new(4, 0)
        );
    }

    #[test]
    fn test_allocator_fractions_compactness() {
        // Two 0.75 does not gives 1.5
        let descriptor = simple_descriptor(1, 2);
        let mut allocator = test_allocator(&descriptor);
        allocator.reset_temporaries(None);
        let rq1 = ResBuilder::default()
            .add(0, ResourceAmount::new(0, 7500))
            .finish_v();
        let rq2 = ResBuilder::default()
            .add(0, ResourceAmount::new(0, 2500))
            .finish_v();

        let al1 = allocator.try_allocate(&rq1).unwrap().0;
        let al2 = allocator.try_allocate(&rq1).unwrap().0;
        let al3 = allocator.try_allocate(&rq2).unwrap().0;
        let al4 = allocator.try_allocate(&rq2).unwrap().0;
        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new(0, 0)
        );
        allocator.release_allocation(al1);
        allocator.release_allocation(al2);

        allocator.reset_temporaries(None);

        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new(1, 5000)
        );

        let rq3 = ResBuilder::default()
            .add(0, ResourceAmount::new(1, 5000))
            .finish_v();
        assert!(allocator.try_allocate(&rq3).is_none());
        allocator.release_allocation(al4);
        allocator.reset_temporaries(None);
        let al5 = allocator.try_allocate(&rq3).unwrap().0;
        allocator.release_allocation(al3);
        allocator.release_allocation(al5);

        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new(2, 0)
        );
    }

    #[test]
    fn test_allocator_groups_and_fractions_scatter() {
        let descriptor = simple_descriptor(3, 2);
        let mut allocator = test_allocator(&descriptor);
        allocator.reset_temporaries(None);
        let rq1 = ResBuilder::default()
            .add_scatter(0, ResourceAmount::new(6, 1))
            .finish_v();
        assert!(allocator.try_allocate(&rq1).is_none());

        let rq1 = ResBuilder::default()
            .add_scatter(0, ResourceAmount::new(2, 5000))
            .finish_v();
        let al1 = allocator.try_allocate(&rq1).unwrap().0;
        let al2 = allocator.try_allocate(&rq1).unwrap().0;
        allocator.validate();
        let r1 = &al1.resources[0].indices;
        let r2 = &al2.resources[0].indices;
        assert_eq!(r1[2].fractions, 5000);
        assert_eq!(r2[2].fractions, 5000);
        assert_eq!(r1[2].group_idx, r2[2].group_idx);
        allocator.release_allocation(al1);
        allocator.release_allocation(al2);
        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new_units(6)
        );
    }

    #[test]
    fn test_allocator_groups_and_fractions() {
        let descriptor = simple_descriptor(3, 2);
        let mut allocator = test_allocator(&descriptor);
        allocator.reset_temporaries(None);
        let rq1 = ResBuilder::default()
            .add(0, ResourceAmount::new(6, 1))
            .finish_v();
        assert!(allocator.try_allocate(&rq1).is_none());

        let rq1 = ResBuilder::default()
            .add_compact(0, ResourceAmount::new(3, 5000))
            .finish_v();
        let al1 = allocator.try_allocate(&rq1).unwrap().0;
        // pools [[1 1] [1 0.5] [0 0]

        let r1 = &al1.resources[0].indices;
        assert_eq!(r1.len(), 4);
        assert_eq!(r1[0].group_idx, r1[1].group_idx);
        assert_eq!(r1[2].group_idx, r1[3].group_idx);
        assert_ne!(r1[0].group_idx, r1[2].group_idx);

        let rq2 = ResBuilder::default()
            .add_compact(0, ResourceAmount::new(0, 4000))
            .finish_v();
        let al2 = allocator.try_allocate(&rq2).unwrap().0;
        // pools [[1 1] [0.1 1] [0 0]

        let r2 = &al2.resources[0].indices;
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].group_idx, r1[2].group_idx);

        allocator.release_allocation(al1);
        // pools [[1 1] [0.6 1] [1 1]

        let rq3 = ResBuilder::default()
            .add_compact(0, ResourceAmount::new(2, 8000))
            .finish_v();
        let al3 = allocator.try_allocate(&rq3).unwrap().0;
        // pools [[1 1] [0.6 0.2] [0 0]

        let r3 = &al3.resources[0].indices;
        assert_eq!(r3.len(), 3);
        assert_eq!(r3[0].group_idx, r3[1].group_idx);
        assert_ne!(r3[0].group_idx, r3[2].group_idx);
        assert_ne!(r3[0].group_idx, r2[0].group_idx);
        assert_ne!(r3[1].group_idx, r2[0].group_idx);
        assert_eq!(r3[2].group_idx, r2[0].group_idx);
        assert_ne!(r3[2].index, r2[0].index);

        let rq4 = ResBuilder::default()
            .add_compact(0, ResourceAmount::new(0, 7000))
            .finish_v();
        let al4 = allocator.try_allocate(&rq4).unwrap().0;
        // pools [[1 0.3] [0.6 0.2] [0 0]
        allocator.validate();

        allocator.release_allocation(al2);
        // pools [[1 0.3] [1 0.2] [0 0]

        let rq6 = ResBuilder::default()
            .add_compact(0, ResourceAmount::new(2, 3000))
            .finish_v();
        let al6 = allocator.try_allocate(&rq6).unwrap().0;
        let r5 = &al6.resources[0].indices;
        assert_eq!(r5.len(), 3);
        assert_eq!(r5[0].fractions, 0);
        assert_eq!(r5[1].fractions, 0);
        assert_eq!(r5[2].fractions, 3000);
        assert_eq!(r5[0].group_idx, r5[2].group_idx);
        assert_ne!(r5[1].group_idx, r5[0].group_idx);
        allocator.validate();

        allocator.release_allocation(al3);
        allocator.release_allocation(al4);
        allocator.release_allocation(al6);

        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new_units(6)
        );
    }

    #[test]
    fn test_allocator_sum_fractions() {
        let descriptor = ResourceDescriptor::new(vec![ResourceDescriptorItem {
            name: "cpus".to_string(),
            kind: ResourceDescriptorKind::Sum {
                size: ResourceAmount::new_units(2),
            },
        }]);
        let mut allocator = test_allocator(&descriptor);
        allocator.reset_temporaries(None);
        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(2, 3000))
            .finish_v();
        assert!(allocator.try_allocate(&rq).is_none());

        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(1, 3000))
            .finish_v();
        let al1 = allocator.try_allocate(&rq).unwrap().0;
        assert_eq!(al1.resources[0].indices.len(), 0);
        assert_eq!(al1.resources[0].amount, ResourceAmount::new(1, 3000));
        allocator.validate();

        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(0, 7001))
            .finish_v();
        assert!(allocator.try_allocate(&rq).is_none());

        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(0, 7000))
            .finish_v();
        let al2 = allocator.try_allocate(&rq).unwrap().0;
        assert_eq!(al2.resources[0].indices.len(), 0);
        assert_eq!(al2.resources[0].amount, ResourceAmount::new(0, 7000));

        allocator.release_allocation(al1);

        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(2, 0))
            .finish_v();
        assert!(allocator.try_allocate(&rq).is_none());
        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(1, 3001))
            .finish_v();
        assert!(allocator.try_allocate(&rq).is_none());

        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(1, 0))
            .finish_v();
        let al3 = allocator.try_allocate(&rq).unwrap().0;

        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new(0, 2000))
            .finish_v();
        let al4 = allocator.try_allocate(&rq).unwrap().0;

        allocator.release_allocation(al4);
        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new(0, 3000)
        );
        allocator.release_allocation(al2);
        allocator.release_allocation(al3);
        assert_eq!(
            allocator.pools[0.into()].concise_state().amount_sum(),
            ResourceAmount::new_units(2)
        )
    }
}
