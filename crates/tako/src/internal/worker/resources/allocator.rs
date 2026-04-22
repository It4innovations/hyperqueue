use crate::internal::common::resources::ResourceVec;
use crate::internal::common::resources::request::{
    AllocationRequest, ResourceAllocRequest, ResourceRequest,
};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::resources::concise::ConciseFreeResources;
use crate::internal::worker::resources::groups::{CouplingWeightItem, group_solver};
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::internal::worker::resources::pool::{FAST_MAX_COUPLED_RESOURCES, ResourcePool};
use crate::resources::{Allocation, ResourceDescriptor, ResourceIdMap};
use smallvec::SmallVec;
use std::cell::RefCell;
use std::rc::Rc;

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
    pub(super) static_info: AllocatorStaticInfo,
    pub(super) own_resources: WorkerResources,
}

impl ResourceAllocator {
    pub fn new(
        desc: &ResourceDescriptor,
        resource_map: &ResourceIdMap,
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
            own_resources: WorkerResources::from_description(desc, resource_map),
        }
    }

    pub fn is_capable_to_run(&self, request: &ResourceRequest) -> bool {
        self.own_resources.is_capable_to_run_request(request)
    }

    fn release_allocation_helper(&mut self, allocation: &Allocation) {
        for al in &allocation.resources {
            self.pools[al.resource_id].release_allocation(al);
        }
    }

    pub fn release_allocation(&mut self, allocation: Rc<Allocation>) {
        self.free_resources.add(&allocation);
        self.release_allocation_helper(&allocation);
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
        for (entry, group_set) in coupling.into_iter().zip(groups) {
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

    pub(crate) fn is_enabled(&self, request: &ResourceRequest) -> bool {
        Self::has_resources_for_request(
            &self.pools,
            &self.free_resources,
            request,
            &self.static_info,
        )
    }

    pub(crate) fn try_allocate(&mut self, request: &ResourceRequest) -> Option<Rc<Allocation>> {
        if !Self::has_resources_for_request(
            &self.pools,
            &self.free_resources,
            request,
            &self.static_info,
        ) {
            return None;
        }
        let allocation = self.claim_resources(request);
        self.free_resources.remove(&allocation);
        Some(Rc::new(allocation))
    }

    pub fn validate(&self) {
        #[cfg(debug_assertions)]
        for (pool, state) in self.pools.iter().zip(self.free_resources.all_states()) {
            pool.validate();
            assert_eq!(pool.concise_state().strip_zeros(), state.strip_zeros());
        }
    }
}
