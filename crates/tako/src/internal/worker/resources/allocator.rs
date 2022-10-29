use crate::internal::common::resources::request::{
    AllocationRequest, ResourceRequest, ResourceRequestEntry,
};
use crate::internal::common::resources::{
    ResourceAllocation, ResourceAllocations, ResourceId, ResourceVec,
};
use crate::internal::worker::resources::counts::{
    resource_count_add_at, ResourceCount, ResourceCountVec,
};
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::internal::worker::resources::pool::ResourcePool;
use crate::resources::{Allocation, ResourceAmount, ResourceDescriptor, ResourceMap};
use smallvec::smallvec;
use std::time::Duration;

pub struct ResourceAllocator {
    pools: ResourceVec<ResourcePool>,
    free_resources: ResourceCountVec,
    remaining_time: Option<Duration>,

    blocked_requests: Vec<BlockedRequest>,
    higher_priority_blocked_requests: usize,
    running_tasks: Vec<ResourceCountVec>, // TODO: Rework on multiset?
}

/// Resource request that cannot be scheduled in the current free resources
#[derive(Debug)]
struct BlockedRequest {
    request: ResourceRequest,
    /// Reachable state of free resources AFTER some of currently running tasks is finished AND
    /// this resource request is enabled
    witnesses: Vec<ResourceCountVec>,
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

        let free_resources =
            ResourceCountVec::new(pools.iter().map(|p| p.count()).collect::<Vec<_>>().into());

        ResourceAllocator {
            pools,
            free_resources,
            remaining_time: None,
            blocked_requests: Vec::new(),
            higher_priority_blocked_requests: 0,
            running_tasks: Vec::new(),
        }
    }

    pub fn init_allocator(&mut self, remaining_time: Option<Duration>) {
        self.remaining_time = remaining_time;
        self.higher_priority_blocked_requests = 0;
        self.blocked_requests.clear();
    }

    fn claim_resources(&mut self, counts: ResourceCountVec) -> Allocation {
        assert!(self.pools.len() >= counts.len());
        let mut allocations = ResourceAllocations::new();
        for (i, (pool, count)) in self.pools.iter_mut().zip(counts.all_counts()).enumerate() {
            if count.iter().sum::<ResourceAmount>() > 0 {
                allocations.push(ResourceAllocation {
                    resource: ResourceId::new(i as u32),
                    value: pool.claim_resources(count),
                });
            }
        }
        Allocation::new(Vec::new(), allocations, counts)
    }

    pub fn release_allocation(&mut self, allocation: Allocation) {
        for al in allocation.resources {
            self.pools[al.resource].release_allocation(al.value);
        }
        self.free_resources.add(&allocation.counts);
        let position = self
            .running_tasks
            .iter()
            .position(|c| c == &allocation.counts)
            .unwrap();
        self.running_tasks.swap_remove(position);
    }

    pub fn take_running_allocations(self) -> Vec<ResourceCountVec> {
        self.running_tasks
    }

    pub fn close_priority_level(&mut self) {
        self.higher_priority_blocked_requests = self.blocked_requests.len();
    }

    fn has_resources_for_entry(
        pool: &ResourcePool,
        free: &ResourceCount,
        policy: &AllocationRequest,
    ) -> bool {
        let sum = free.iter().sum::<ResourceAmount>();
        match policy {
            AllocationRequest::Compact(amount) | AllocationRequest::Scatter(amount) => {
                *amount <= sum
            }
            AllocationRequest::ForceCompact(amount) => {
                if *amount > sum {
                    return false;
                }
                if *amount == 0 {
                    return true;
                }
                let socket_size = (((amount - 1) / pool.min_group_size()) as usize) + 1;
                if free.len() < socket_size {
                    return false;
                }
                let mut free = free.clone();
                free.sort_unstable();
                let sum = free.iter().rev().take(socket_size).sum();
                *amount <= sum
            }
            AllocationRequest::All => sum == pool.full_size(),
        }
    }

    fn try_allocate_compact(free: &mut ResourceCount, mut amount: ResourceAmount) -> ResourceCount {
        let mut result = ResourceCount::new();
        loop {
            if let Some((i, c)) = free
                .iter_mut()
                .enumerate()
                .filter(|(_i, c)| **c >= amount)
                .min_by_key(|(_i, c)| **c)
            {
                resource_count_add_at(&mut result, i, amount);
                *c -= amount;
                return result;
            } else {
                let (i, c) = free
                    .iter_mut()
                    .enumerate()
                    .max_by_key(|(_i, c)| **c)
                    .unwrap();
                resource_count_add_at(&mut result, i, *c);
                amount -= *c;
                *c = 0;
            }
        }
    }

    fn try_allocate_scatter(free: &mut ResourceCount, mut amount: ResourceAmount) -> ResourceCount {
        assert!(free.iter().sum::<ResourceAmount>() >= amount);
        let mut result = ResourceCount::new();

        let mut idx = 0;
        while amount > 0 {
            if free[idx] > 0 {
                free[idx] -= 1;
                amount -= 1;
                resource_count_add_at(&mut result, idx, 1);
            }
            idx = (idx + 1) % free.len();
        }
        result
    }

    fn allocate_entry(free: &mut ResourceCount, policy: &AllocationRequest) -> ResourceCount {
        match policy {
            AllocationRequest::Compact(amount)
            | AllocationRequest::ForceCompact(amount)
            | AllocationRequest::Scatter(amount)
                if free.len() == 1 =>
            {
                free[0] -= *amount;
                smallvec![*amount]
            }
            AllocationRequest::Compact(amount) | AllocationRequest::ForceCompact(amount) => {
                Self::try_allocate_compact(free, *amount)
            }
            AllocationRequest::Scatter(amount) => Self::try_allocate_scatter(free, *amount),
            AllocationRequest::All => {
                let result = free.clone();
                free.fill(0);
                result
            }
        }
    }

    fn compute_witness<'b>(
        pools: &[ResourcePool],
        free: &ResourceCountVec,
        request: &ResourceRequest,
        running: impl Iterator<Item = &'b ResourceCountVec>,
    ) -> Option<ResourceCountVec> {
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
        free: &ResourceCountVec,
        request: &ResourceRequest,
    ) -> bool {
        request.entries().iter().all(|entry| {
            Self::has_resources_for_entry(
                &pools[entry.resource_id.as_num() as usize],
                free.get(entry.resource_id),
                &entry.request,
            )
        })
    }

    fn compute_witnesses(
        pools: &[ResourcePool],
        free_resources: &ResourceCountVec,
        running: &mut [ResourceCountVec],
        request: &ResourceRequest,
    ) -> Vec<ResourceCountVec> {
        let mut full = free_resources.clone();
        for running in running.iter() {
            full.add(running);
        }

        let mut witnesses = Vec::with_capacity(free_resources.len() * 2);

        let compute_witnesses = |witnesses: &mut Vec<ResourceCountVec>,
                                 running: &[ResourceCountVec]| {
            let w = Self::compute_witness(pools, free_resources, request, running.iter());
            witnesses.push(w.unwrap());
            let w = Self::compute_witness(pools, free_resources, request, running.iter().rev());
            witnesses.push(w.unwrap());
        };

        for i in 0..free_resources.len() {
            let idx = ResourceId::from(i as u32);
            running.sort_unstable_by_key(|x| {
                (
                    x.get(idx).iter().sum::<ResourceAmount>(),
                    (x.fraction(&full) * 10_000.0) as u32,
                )
            });
            running.sort_unstable_by_key(|x| {
                (
                    x.get(idx).iter().sum::<ResourceAmount>(),
                    -(x.fraction(&full) * 10_000.0) as u32,
                )
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

    fn check_blocked_request(&mut self, allocation: &ResourceCountVec) -> bool {
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
                assert!(free.remove(allocation));
                if !Self::has_resources_for_request(&self.pools, &free, &blocked.request) {
                    return false;
                }
            }
        }
        true
    }

    pub fn try_allocate(&mut self, request: &ResourceRequest) -> Option<Allocation> {
        self.try_allocate_counts(request)
            .map(|c| self.claim_resources(c))
    }

    fn try_allocate_counts(&mut self, request: &ResourceRequest) -> Option<ResourceCountVec> {
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

        let mut allocation = ResourceCountVec::default();
        for entry in request.entries() {
            allocation.set(
                entry.resource_id,
                Self::allocate_entry(
                    self.free_resources.get_mut(entry.resource_id),
                    &entry.request,
                ),
            )
        }

        if !self.check_blocked_request(&allocation) {
            self.free_resources.add(&allocation);
            return None;
        }

        self.running_tasks.push(allocation.clone());
        Some(allocation)
    }

    pub fn difficulty_score(&self, entry: &ResourceRequestEntry) -> f32 {
        let size = self
            .pools
            .get(entry.resource_id.as_num() as usize)
            .map(|x| x.full_size())
            .unwrap_or(0);
        if size == 0 {
            0.0f32
        } else {
            match entry.request {
                AllocationRequest::Compact(amount) | AllocationRequest::Scatter(amount) => {
                    amount as f32 / size as f32
                }
                AllocationRequest::ForceCompact(amount)
                    if self.pools[entry.resource_id].n_groups() == 1 =>
                {
                    amount as f32 / size as f32
                }
                AllocationRequest::ForceCompact(amount) => (amount * 2) as f32 / size as f32,
                AllocationRequest::All => 2.0,
            }
        }
    }

    pub fn validate(&self) {
        #[cfg(debug_assertions)]
        for (pool, count) in self.pools.iter().zip(self.free_resources.all_counts()) {
            pool.validate();
            assert_eq!(&pool.count(), count);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::descriptor::{
        ResourceDescriptor, ResourceDescriptorKind,
    };
    use crate::internal::common::resources::{
        Allocation, AllocationValue, ResourceId, ResourceRequest,
    };
    use crate::internal::tests::utils::resources::{cpus_compact, ResBuilder};
    use crate::internal::tests::utils::shared::res_allocator_from_descriptor;
    use crate::internal::tests::utils::sorted_vec;
    use crate::internal::worker::resources::allocator::ResourceAllocator;
    use crate::internal::worker::resources::counts::ResourceCountVec;
    use crate::internal::worker::resources::pool::ResourcePool;
    use crate::resources::{ResourceAmount, ResourceDescriptorItem};
    use smallvec::smallvec;
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
        n_sockets: ResourceAmount,
        socket_size: ResourceAmount,
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
        free: &[ResourceAmount],
        running: &[&[ResourceAmount]],
        remaining_time: Option<Duration>,
    ) -> ResourceAllocator {
        let mut names = vec!["cpus".to_string()];
        names.extend((1..free.len()).map(|i| format!("res{}", i)));
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
                    kind: ResourceDescriptorKind::simple_indices(total as u32),
                }
            })
            .collect();
        let descriptor = ResourceDescriptor::new(ds);

        let mut ac = res_allocator_from_descriptor(descriptor);
        for r in running {
            let c = ResourceCountVec::new_simple(r);
            assert!(ac.free_resources.remove(&c));
            ac.claim_resources(c.clone());
            ac.running_tasks.push(c);
        }
        ac.init_allocator(remaining_time);
        ac
    }

    fn simple_alloc(
        allocator: &mut ResourceAllocator,
        counts: &[ResourceAmount],
        expect_pass: bool,
    ) {
        let mut builder = ResBuilder::default();
        for (i, count) in counts.iter().enumerate() {
            if *count > 0 {
                builder = builder.add(ResourceId::from(i as u32), *count);
            }
        }
        let al = allocator.try_allocate_counts(&builder.finish());
        if expect_pass {
            let r = al.unwrap();
            r.assert_eq(counts);
            allocator.claim_resources(r);
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
        let free = ResourceCountVec::new_simple(&[1, 2]);
        let rq = ResBuilder::default().add(0, 3).add(1, 1).finish();

        assert!(ResourceAllocator::compute_witness(&pools, &free, &rq, [].iter()).is_none());
        assert!(ResourceAllocator::compute_witness(
            &pools,
            &mut free.clone(),
            &rq,
            [ResourceCountVec::new_simple(&[1])].iter()
        )
        .is_none());
        assert_eq!(
            ResourceAllocator::compute_witness(
                &pools,
                &free,
                &rq,
                [
                    ResourceCountVec::new_simple(&[1]),
                    ResourceCountVec::new_simple(&[1]),
                    ResourceCountVec::new_simple(&[1])
                ]
                .iter()
            ),
            Some(ResourceCountVec::new_simple(&[3, 2]))
        );

        let free = ResourceCountVec::new_simple(&[4, 2]);
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
                    ResourceCountVec::new_simple(&[2]),
                    ResourceCountVec::new_simple(&[2]),
                    ResourceCountVec::new_simple(&[2]),
                    ResourceCountVec::new_simple(&[0, 3]),
                    ResourceCountVec::new_simple(&[0, 3])
                ]
                .iter()
            ),
            Some(ResourceCountVec::new_simple(&[10, 5]))
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

        let rq = cpus_compact(3).finish();
        let al = allocator.try_allocate(&rq).unwrap();
        assert_eq!(al.resources.len(), 1);
        assert_eq!(al.resources[0].resource, ResourceId::new(0));
        assert_eq!(al.resources[0].value.get_checked_indices().len(), 3);
        assert!(al.resources[0]
            .value
            .get_checked_indices()
            .iter()
            .all(|x| *x < 4));

        let rq = cpus_compact(2).finish();
        assert!(allocator.try_allocate(&rq).is_none());

        assert_eq!(allocator.running_tasks.len(), 1);
        allocator.release_allocation(al);
        assert_eq!(allocator.running_tasks.len(), 0);

        allocator.init_allocator(None);

        let rq = cpus_compact(4).finish();
        let al = allocator.try_allocate(&rq).unwrap();
        assert_eq!(
            al.resources[0].value.get_checked_indices(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(allocator.running_tasks.len(), 1);
        allocator.release_allocation(al);
        assert_eq!(allocator.running_tasks.len(), 0);
        assert_eq!(allocator.free_resources.get(0.into())[0], 4);
        let v: Vec<ResourceAmount> = vec![4];
        assert_eq!(allocator.pools[ResourceId::new(0)].count().to_vec(), v);

        allocator.init_allocator(None);

        let rq = cpus_compact(1).finish();
        let rq2 = cpus_compact(2).finish();
        let al1 = allocator.try_allocate(&rq).unwrap();
        let al2 = allocator.try_allocate(&rq).unwrap();
        let al3 = allocator.try_allocate(&rq).unwrap();
        let al4 = allocator.try_allocate(&rq).unwrap();
        assert!(allocator.try_allocate(&rq).is_none());
        assert!(allocator.try_allocate(&rq2).is_none());

        allocator.release_allocation(al2);
        allocator.release_allocation(al4);

        allocator.init_allocator(None);

        let al5 = allocator.try_allocate(&rq2).unwrap();

        assert!(allocator.try_allocate(&rq).is_none());
        assert!(allocator.try_allocate(&rq2).is_none());

        let mut v = Vec::new();
        v.append(&mut al1.resources[0].value.get_checked_indices());
        assert_eq!(v.len(), 1);
        v.append(&mut al5.resources[0].value.get_checked_indices());
        assert_eq!(v.len(), 3);
        v.append(&mut al3.resources[0].value.get_checked_indices());
        assert_eq!(sorted_vec(v), vec![0, 1, 2, 3]);
        allocator.validate();
    }

    #[test]
    fn test_pool_compact1() {
        let descriptor = simple_descriptor(4, 6);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = cpus_compact(4).finish();
        let al1 = allocator.try_allocate(&rq1).unwrap();
        let s1 = allocator.get_sockets(&al1, 0);
        assert_eq!(s1.len(), 1);
        let al2 = allocator.try_allocate(&rq1).unwrap();
        let s2 = allocator.get_sockets(&al2, 0);
        assert_eq!(s2.len(), 1);
        assert_ne!(s1, s2);

        let rq2 = cpus_compact(3).finish();
        let al3 = allocator.try_allocate(&rq2).unwrap();
        let s3 = allocator.get_sockets(&al3, 0);
        assert_eq!(s3.len(), 1);
        let al4 = allocator.try_allocate(&rq2).unwrap();
        let s4 = allocator.get_sockets(&al4, 0);
        assert_eq!(s4.len(), 1);
        assert_ne!(s3, s1);
        assert_ne!(s4, s1);
        assert_ne!(s3, s2);
        assert_ne!(s4, s2);
        assert_eq!(s3, s4);

        allocator.close_priority_level();

        let rq3 = cpus_compact(6).finish();
        let al = allocator.try_allocate(&rq3).unwrap();
        assert_eq!(allocator.get_sockets(&al, 0).len(), 1);
        allocator.release_allocation(al);
        allocator.init_allocator(None);

        let rq3 = cpus_compact(7).finish();
        let al = allocator.try_allocate(&rq3).unwrap();
        assert_eq!(allocator.get_sockets(&al, 0).len(), 2);

        allocator.release_allocation(al);
        allocator.init_allocator(None);

        let rq3 = cpus_compact(8).finish();
        let al = allocator.try_allocate(&rq3).unwrap();
        assert_eq!(allocator.get_sockets(&al, 0).len(), 2);
        allocator.release_allocation(al);
        allocator.init_allocator(None);

        let rq3 = cpus_compact(9).finish();
        let al = allocator.try_allocate(&rq3).unwrap();
        assert_eq!(allocator.get_sockets(&al, 0).len(), 3);
        allocator.release_allocation(al);
        allocator.validate();
    }

    #[test]
    fn test_pool_allocate_compact_all() {
        let descriptor = simple_descriptor(4, 6);
        let mut allocator = test_allocator(&descriptor);

        let rq = cpus_compact(24).finish();
        let al = allocator.try_allocate(&rq).unwrap();
        assert_eq!(
            al.resource_allocation(0.into())
                .unwrap()
                .value
                .get_checked_indices(),
            (0..24u32).collect::<Vec<_>>()
        );
        assert_eq!(allocator.get_current_free(0), 0);
        allocator.release_allocation(al);
        assert_eq!(allocator.get_current_free(0), 24);
        allocator.validate();
    }

    #[test]
    fn test_pool_allocate_all() {
        let descriptor = simple_descriptor(4, 6);
        let mut allocator = test_allocator(&descriptor);

        let rq = ResBuilder::default().add_all(0).finish();
        let al = allocator.try_allocate(&rq).unwrap();
        assert_eq!(
            al.resource_allocation(0.into())
                .unwrap()
                .value
                .get_checked_indices(),
            (0..24u32).collect::<Vec<_>>()
        );
        assert_eq!(allocator.get_current_free(0), 0);
        allocator.release_allocation(al);
        assert_eq!(allocator.get_current_free(0), 24);

        allocator.init_allocator(None);

        let rq2 = cpus_compact(1).finish();
        assert!(allocator.try_allocate(&rq2).is_some());
        assert!(allocator.try_allocate(&rq).is_none());
        allocator.validate();
    }

    #[test]
    fn test_pool_force_compact1() {
        let descriptor = simple_descriptor(2, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_force_compact(0, 9).finish();
        assert!(allocator.try_allocate(&rq1).is_none());

        let rq2 = ResBuilder::default().add_force_compact(0, 2).finish();
        for _ in 0..4 {
            let al1 = allocator.try_allocate(&rq2).unwrap();
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

        let rq1 = ResBuilder::default().add_force_compact(0, 3).finish();
        for _ in 0..2 {
            let al1 = allocator.try_allocate(&rq1).unwrap();
            assert_eq!(al1.get_indices(0).len(), 3);
            assert_eq!(allocator.get_sockets(&al1, 0).len(), 1);
        }

        let rq1 = ResBuilder::default().add_force_compact(0, 2).finish();
        assert!(allocator.try_allocate(&rq1).is_none());

        let rq1 = cpus_compact(2).finish();
        assert!(allocator.try_allocate(&rq1).is_some());
        allocator.validate();
    }

    #[test]
    fn test_pool_force_compact3() {
        let descriptor = simple_descriptor(3, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_force_compact(0, 8).finish();
        let al1 = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 8);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);
        allocator.release_allocation(al1);
        allocator.validate();

        allocator.init_allocator(None);

        let rq1 = ResBuilder::default().add_force_compact(0, 5).finish();
        let al1 = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 5);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);
        allocator.release_allocation(al1);
        allocator.validate();

        allocator.init_allocator(None);

        let rq1 = ResBuilder::default().add_force_compact(0, 10).finish();
        let al1 = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 10);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);
        allocator.release_allocation(al1);
        allocator.validate();
    }

    #[test]
    fn test_pool_force_scatter1() {
        let descriptor = simple_descriptor(3, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_scatter(0, 3).finish();
        let al1 = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 3);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);

        let rq1 = ResBuilder::default().add_scatter(0, 4).finish();
        let al1 = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 4);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);

        let rq1 = ResBuilder::default().add_scatter(0, 2).finish();
        let al1 = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 2);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);

        allocator.validate();
    }

    #[test]
    fn test_pool_force_scatter2() {
        let descriptor = simple_descriptor(3, 4);
        let mut allocator = test_allocator(&descriptor);

        let rq1 = ResBuilder::default().add_force_compact(0, 4).finish();
        let _al1 = allocator.try_allocate(&rq1).unwrap();

        let rq2 = ResBuilder::default().add_scatter(0, 5).finish();
        let al2 = allocator.try_allocate(&rq2).unwrap();
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
                kind: ResourceDescriptorKind::Sum { size: 100_000_000 },
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
            ResourceCountVec::new(
                vec![
                    smallvec![4],
                    smallvec![96],
                    smallvec![100_000_000],
                    smallvec![2],
                    smallvec![2]
                ]
                .into()
            )
        );

        let rq: ResourceRequest = cpus_compact(1)
            .add(4, 1)
            .add(1, 12)
            .add(2, 1000_000)
            .finish();
        rq.validate().unwrap();
        let al = allocator.try_allocate(&rq).unwrap();
        assert_eq!(al.resources.len(), 4);
        assert_eq!(al.resources[0].resource.as_num(), 0);

        assert_eq!(al.resources[1].resource.as_num(), 1);
        assert!(matches!(
            &al.resources[1].value,
            AllocationValue::Indices(indices) if indices.len() == 12
        ));

        assert_eq!(al.resources[2].resource.as_num(), 2);
        assert!(matches!(
            &al.resources[2].value,
            AllocationValue::Sum(1000_000)
        ));

        assert_eq!(al.resources[3].resource.as_num(), 4);
        assert!(matches!(
            &al.resources[3].value,
            AllocationValue::Indices(indices) if indices.len() == 1
        ));

        assert_eq!(allocator.get_current_free(1), 84);
        assert_eq!(allocator.get_current_free(2), 99_000_000);
        assert_eq!(allocator.get_current_free(3), 2);
        assert_eq!(allocator.get_current_free(4), 1);

        let rq: ResourceRequest = cpus_compact(1).add(4, 2).finish();
        assert!(allocator.try_allocate(&rq).is_none());

        allocator.release_allocation(al);

        assert_eq!(allocator.get_current_free(1), 96);
        assert_eq!(allocator.get_current_free(2), 100_000_000);
        assert_eq!(allocator.get_current_free(3), 2);
        assert_eq!(allocator.get_current_free(4), 2);

        allocator.init_allocator(None);
        assert!(allocator.try_allocate(&rq).is_some());

        allocator.validate();
    }

    #[test]
    fn test_allocator_remaining_time_no_known() {
        let descriptor = simple_descriptor(1, 4);
        let mut allocator = test_allocator(&descriptor);

        allocator.init_allocator(None);
        let rq = ResBuilder::default().add(0, 1).min_time_secs(100).finish();
        let al = allocator.try_allocate(&rq).unwrap();
        allocator.release_allocation(al);

        allocator.init_allocator(Some(Duration::from_secs(101)));
        let al = allocator.try_allocate(&rq).unwrap();
        allocator.release_allocation(al);

        allocator.init_allocator(Some(Duration::from_secs(99)));
        assert!(allocator.try_allocate(&rq).is_none());

        allocator.validate();
    }
}
