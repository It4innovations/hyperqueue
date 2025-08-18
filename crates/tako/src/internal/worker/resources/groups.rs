use crate::internal::common::resources::ResourceId;
use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
use crate::internal::worker::resources::pool::{FAST_MAX_COUPLED_RESOURCES, FAST_MAX_GROUPS};
use crate::resources::{AllocationRequest, ResourceAmount, ResourceGroupIdx};
use smallvec::SmallVec;

pub(crate) struct CouplingWeightItem {
    pub(crate) resource1: ResourceId,
    pub(crate) group1: ResourceGroupIdx,
    pub(crate) resource2: ResourceId,
    pub(crate) group2: ResourceGroupIdx,
    pub(crate) weight: u16,
}

struct GroupMinimizationState {
    request: ResourceAmount,
    group_amounts: SmallVec<[ResourceAmount; FAST_MAX_GROUPS]>,
}

type GroupSet = SmallVec<[usize; 2]>;

fn group_solver(states: &mut [GroupMinimizationState]) -> Option<GroupSet> {
    todo!()
}

pub fn find_coupled_groups(
    n_groups: usize,
    free: &ConciseFreeResources,
    entries: &[&crate::resources::ResourceAllocRequest],
) -> Option<GroupSet> {
    let mut states = entries
        .iter()
        .map(|e| {
            let remaining = e.request.amount_or_none_if_all().unwrap();
            GroupMinimizationState {
                request: remaining,
                group_amounts: free.get(e.resource_id).amount_max_per_group().collect(),
            }
        })
        .collect::<SmallVec<[GroupMinimizationState; FAST_MAX_COUPLED_RESOURCES]>>();
    group_solver(n_groups, &mut states)
}

pub fn find_compact_groups(
    n_groups: usize,
    free: &ConciseResourceState,
    policy: &AllocationRequest,
) -> Option<GroupSet> {
    let remaining = policy.amount_or_none_if_all().unwrap();
    let mut states = [GroupMinimizationState {
        request: remaining,
        group_amounts: free.amount_max_per_group().collect(),
    }];
    group_solver(n_groups, &mut states)
}
