use crate::internal::common::resources::request::{
    AllocationRequest, ResourceAllocRequest, ResourceRequest, ResourceRequestVariants,
};
use crate::internal::common::resources::{ResourceId, ResourceVec};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::resources::concise::ConciseFreeResources;
use crate::internal::worker::resources::groups::{CouplingWeightItem, group_solver};
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::internal::worker::resources::pool::{FAST_MAX_COUPLED_RESOURCES, ResourcePool};
use crate::resources::{Allocation, ResourceAmount, ResourceDescriptor, ResourceMap};
use smallvec::SmallVec;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

pub(crate) struct AllocatorStaticInfo {
    pub(super) coupling_weights: Vec<CouplingWeightItem>,
    /// This stores optimal objective values for each request containing ForceCompact/ForceTight requests
    /// If we can find a solution that is currently best but not with an objective value larger than this value,
    /// then ForceCompact/ForceTight requests are not considered enabled.
    pub(super) optional_objectives: RefCell<crate::Map<ResourceRequest, f64>>,
    /// Concise resource representation of all resources used for computing optimal objective values
    pub(super) all_resources: ConciseFreeResources,
}

pub struct ResourceAllocator {
    pub(super) pools: ResourceVec<ResourcePool>,
    pub(super) free_resources: ConciseFreeResources,
    pub(super) remaining_time: Option<Duration>,
    pub(super) static_info: AllocatorStaticInfo,
    blocked_requests: Vec<BlockedRequest>,
    higher_priority_blocked_requests: usize,
    pub(super) running_tasks: Vec<Rc<Allocation>>, // TODO: Rework on multiset?
    pub(super) own_resources: WorkerResources,
}

/// Resource request that cannot be scheduled in the current free resources
#[derive(Debug)]
struct BlockedRequest {
    request: ResourceRequest,
    /// Reachable state of free resources AFTER some of the currently running tasks is finished AND
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

        let coupling_weights = desc
            .coupling
            .weights
            .iter()
            .map(|w| {
                let resource1 = resource_map
                    .get_index(&desc.resources[w.resource1_idx as usize].name)
                    .unwrap();
                let resource2 = resource_map
                    .get_index(&desc.resources[w.resource2_idx as usize].name)
                    .unwrap();
                CouplingWeightItem {
                    resource1,
                    group1: w.group1_idx,
                    resource2,
                    group2: w.group2_idx,
                    weight: w.weight as f64,
                }
            })
            .collect();

        let static_info = AllocatorStaticInfo {
            coupling_weights,
            optional_objectives: RefCell::new(Default::default()),
            all_resources: free_resources.clone(),
        };

        ResourceAllocator {
            pools,
            free_resources,
            static_info,
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

    pub(super) fn compute_witness<'b>(
        pools: &[ResourcePool],
        free: &ConciseFreeResources,
        request: &ResourceRequest,
        running: impl Iterator<Item = &'b Rc<Allocation>>,
        static_info: &AllocatorStaticInfo,
    ) -> Option<ConciseFreeResources> {
        let mut free = free.clone();
        if Self::has_resources_for_request(pools, &free, request, static_info) {
            return Some(free);
        }
        for running_request in running {
            free.add(running_request);
            if Self::has_resources_for_request(pools, &free, request, static_info) {
                return Some(free);
            }
        }
        None
    }

    fn has_resources_for_request(
        pools: &[ResourcePool],
        free: &ConciseFreeResources,
        request: &ResourceRequest,
        static_info: &AllocatorStaticInfo,
    ) -> bool {
        let mut coupling: SmallVec<[&ResourceAllocRequest; FAST_MAX_COUPLED_RESOURCES]> =
            SmallVec::new();
        if !request.entries().iter().all(|entry| {
            let Some(pool) = pools.get(entry.resource_id.as_usize()) else {
                return false;
            };
            if let ResourcePool::Groups(_) = pool
                && entry.request.is_relevant_for_coupling()
            {
                coupling.push(entry);
            }
            let max_alloc = free.get(entry.resource_id).amount_max_alloc();
            match &entry.request {
                AllocationRequest::Compact(amount)
                | AllocationRequest::Tight(amount)
                | AllocationRequest::Scatter(amount)
                | AllocationRequest::ForceCompact(amount)
                | AllocationRequest::ForceTight(amount) => *amount <= max_alloc,
                AllocationRequest::All => max_alloc == pool.full_size(),
            }
        }) {
            return false;
        }
        if coupling.iter().all(|entry| !entry.request.is_forced()) {
            return true;
        }
        let Some((_, objective_value)) =
            group_solver(free, &coupling, &static_info.coupling_weights)
        else {
            return false;
        };
        let mut optimal_costs = static_info.optional_objectives.borrow_mut();
        let optimal_const = if let Some(cost) = optimal_costs.get(request) {
            *cost
        } else {
            let (_, mut cost) = group_solver(
                &static_info.all_resources,
                &coupling,
                &static_info.coupling_weights,
            )
            .unwrap();
            cost -= 0.1;
            optimal_costs.insert(request.clone(), cost);
            cost
        };
        objective_value >= optimal_const
    }

    fn compute_witnesses(
        pools: &[ResourcePool],
        free_resources: &ConciseFreeResources,
        running: &mut [Rc<Allocation>],
        request: &ResourceRequest,
        static_info: &AllocatorStaticInfo,
    ) -> Vec<ConciseFreeResources> {
        let mut witnesses: Vec<ConciseFreeResources> =
            Vec::with_capacity(2 * free_resources.n_resources());

        let compute_witnesses = |witnesses: &mut Vec<ConciseFreeResources>,
                                 running: &[Rc<Allocation>]| {
            let w =
                Self::compute_witness(pools, free_resources, request, running.iter(), static_info);
            witnesses.push(w.unwrap());
            let w = Self::compute_witness(
                pools,
                free_resources,
                request,
                running.iter().rev(),
                static_info,
            );
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
                    &self.static_info,
                )
            }
            for witness in &blocked.witnesses {
                let mut free = witness.clone();
                free.remove(allocation);
                if !Self::has_resources_for_request(
                    &self.pools,
                    &free,
                    &blocked.request,
                    &self.static_info,
                ) {
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
        let mut coupling: SmallVec<[&ResourceAllocRequest; FAST_MAX_COUPLED_RESOURCES]> =
            SmallVec::new();
        for entry in request.entries() {
            let pool = self.pools.get_mut(entry.resource_id.as_usize()).unwrap();
            if let ResourcePool::Groups(_) = pool
                && entry.request.is_relevant_for_coupling()
            {
                coupling.push(entry);
                continue;
            }
            allocation
                .add_resource_allocation(pool.claim_resources(entry.resource_id, &entry.request))
        }
        if coupling.is_empty() {
            return allocation;
        }
        let (groups, _) = group_solver(
            &self.free_resources,
            &coupling,
            &self.static_info.coupling_weights,
        )
        .unwrap();
        for (entry, group_set) in coupling.into_iter().zip(groups.into_iter()) {
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
        if let Some(remaining_time) = self.remaining_time
            && remaining_time < request.min_time()
        {
            return None;
        }

        if self.blocked_requests.iter().any(|b| &b.request == request) {
            return None;
        }

        if !Self::has_resources_for_request(
            &self.pools,
            &self.free_resources,
            request,
            &self.static_info,
        ) {
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

    pub fn difficulty_score(&self, entry: &ResourceAllocRequest) -> f32 {
        let size = self
            .pools
            .get(entry.resource_id)
            .map(|x| x.full_size())
            .unwrap_or(ResourceAmount::ZERO);
        if size.is_zero() {
            0.0f32
        } else {
            match entry.request {
                AllocationRequest::Compact(amount)
                | AllocationRequest::Scatter(amount)
                | AllocationRequest::Tight(amount) => {
                    amount.total_fractions() as f32 / size.total_fractions() as f32
                }
                AllocationRequest::ForceCompact(amount) | AllocationRequest::ForceTight(amount)
                    if self.pools[entry.resource_id].n_groups() == 1 =>
                {
                    amount.total_fractions() as f32 / size.total_fractions() as f32
                }
                AllocationRequest::ForceCompact(amount) | AllocationRequest::ForceTight(amount) => {
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
