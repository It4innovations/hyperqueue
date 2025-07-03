use crate::internal::common::resources::request::{
    AllocationRequest, ResourceRequest, ResourceRequestEntry, ResourceRequestVariants,
};
use crate::internal::common::resources::{ResourceId, ResourceVec};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
use crate::internal::worker::resources::groups::{find_compact_groups, find_coupled_groups};
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::internal::worker::resources::pool::{FAST_MAX_COUPLED_RESOURCES, ResourcePool};
use crate::resources::{Allocation, ResourceAmount, ResourceDescriptor, ResourceMap};
use smallvec::SmallVec;
use std::rc::Rc;
use std::time::Duration;

pub(crate) struct ResourceCoupling {
    resources: Vec<ResourceId>,
}

pub struct ResourceAllocator {
    pub(super) pools: ResourceVec<ResourcePool>,
    pub(super) free_resources: ConciseFreeResources,
    pub(super) remaining_time: Option<Duration>,
    coupling_n_group_size: usize,
    blocked_requests: Vec<BlockedRequest>,
    higher_priority_blocked_requests: usize,
    pub(super) running_tasks: Vec<Rc<Allocation>>, // TODO: Rework on multiset?
    pub(super) own_resources: WorkerResources,
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
            let is_coupled = desc
                .coupling
                .as_ref()
                .map(|c| c.names.contains(&item.name))
                .unwrap_or(false);
            pools[idx] = ResourcePool::new(&item.kind, idx, label_map, is_coupled);
        }

        let free_resources = ConciseFreeResources::new(
            pools
                .iter()
                .map(|p| p.concise_state())
                .collect::<Vec<_>>()
                .into(),
        );

        let couplings = desc.coupling.as_ref().map(|c| ResourceCoupling {
            resources: c
                .names
                .iter()
                .map(|name| resource_map.get_index(name).unwrap())
                .collect(),
        });
        let coupling_n_group_size = couplings
            .as_ref()
            .and_then(|c| c.resources.first().map(|r| pools[*r].n_groups()))
            .unwrap_or(0);
        ResourceAllocator {
            pools,
            free_resources,
            remaining_time: None,
            coupling_n_group_size,
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
        entry: &ResourceRequestEntry,
    ) -> bool {
        let max_alloc = free.amount_max_alloc();
        match &entry.request {
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
                let units = amount.whole_units();
                let socket_count = (((units - 1) / pool.min_group_size()) as usize) + 1;
                if free.n_groups() < socket_count {
                    return false;
                }
                let Some(groups) = find_compact_groups(pool.n_groups(), free, entry) else {
                    return false;
                };
                groups.len() <= socket_count
            }
            AllocationRequest::All => max_alloc == pool.full_size(),
        }
    }

    pub(super) fn compute_witness<'b>(
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
        let mut coupling: SmallVec<[&ResourceRequestEntry; FAST_MAX_COUPLED_RESOURCES]> =
            SmallVec::new();
        if !request.entries().iter().all(|entry| {
            let pool = &pools[entry.resource_id.as_usize()];
            if let ResourcePool::Groups(g) = pool {
                if g.is_coupled() && entry.request.is_relevant_for_coupling() {
                    coupling.push(entry);
                }
            }
            Self::has_resources_for_entry(pool, free.get(entry.resource_id), entry)
        }) {
            return false;
        }
        if coupling.len() <= 1 || coupling.iter().all(|entry| !entry.request.is_forced()) {
            return true;
        }
        let Some(groups) = find_coupled_groups(
            pools[request.entries()[0].resource_id.as_usize()].n_groups(),
            free,
            &coupling,
        ) else {
            return false;
        };
        coupling.iter().all(|entry| {
            let amount = match entry.request {
                AllocationRequest::ForceCompact(amount) => amount,
                AllocationRequest::Compact(_)
                | AllocationRequest::Scatter(_)
                | AllocationRequest::All => return true,
            };
            let pool = &pools[entry.resource_id.as_usize()];
            pool.min_group_size();
            let units = amount.whole_units();
            let socket_count = (((units - 1) / pool.min_group_size()) as usize) + 1;
            groups.len() <= socket_count
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

    fn claim_resources(&mut self, request: &ResourceRequest) -> Allocation {
        let mut allocation = Allocation::new();
        let mut coupling: SmallVec<[&ResourceRequestEntry; FAST_MAX_COUPLED_RESOURCES]> =
            SmallVec::new();
        for entry in request.entries() {
            let pool = self.pools.get_mut(entry.resource_id.as_usize()).unwrap();
            if let ResourcePool::Groups(g) = pool {
                if g.is_coupled() && entry.request.is_relevant_for_coupling() {
                    coupling.push(entry);
                    continue;
                }
            }
            allocation
                .add_resource_allocation(pool.claim_resources(entry.resource_id, &entry.request))
        }
        if coupling.is_empty() {
            return allocation;
        }
        if coupling.len() == 1 {
            let entry = coupling[0];
            let pool = &mut self.pools[entry.resource_id];
            allocation
                .add_resource_allocation(pool.claim_resources(entry.resource_id, &entry.request));
            allocation.normalize_allocation();
            return allocation;
        }
        let group_set =
            find_coupled_groups(self.coupling_n_group_size, &self.free_resources, &coupling)
                .unwrap();
        for entry in coupling {
            allocation.add_resource_allocation(
                self.pools[entry.resource_id].claim_resources_with_group_mask(
                    entry.resource_id,
                    &entry.request,
                    &group_set,
                ),
            )
        }
        allocation.normalize_allocation();
        allocation
    }

    pub(super) fn try_allocate_variant(
        &mut self,
        request: &ResourceRequest,
    ) -> Option<Rc<Allocation>> {
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
        let allocation = self.claim_resources(request);
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
