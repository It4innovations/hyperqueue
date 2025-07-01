use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
use crate::internal::worker::resources::pool::{FAST_MAX_COUPLED_RESOURCES, FAST_MAX_GROUPS};
use crate::resources::ResourceAmount;
use smallvec::SmallVec;
use std::cmp::Reverse;

struct GroupMinimizationState {
    remaining: ResourceAmount,
    group_amounts: SmallVec<[ResourceAmount; FAST_MAX_GROUPS]>,
}

type GroupSet = SmallVec<[usize; 2]>;

fn group_minimizer(n_groups: usize, states: &mut [GroupMinimizationState]) -> Option<GroupSet> {
    let mut result: GroupSet = SmallVec::new();
    loop {
        if let Some(group_idx) = (0..n_groups)
            .filter(|group_idx| {
                states
                    .iter()
                    .all(|s| s.group_amounts[*group_idx] >= s.remaining)
            })
            .min_by_key(|group_idx| {
                (
                    states
                        .iter()
                        .map(|s| s.group_amounts[*group_idx] - s.remaining)
                        .min()
                        .unwrap_or(ResourceAmount::ZERO),
                    states
                        .iter()
                        .map(|s| s.group_amounts[*group_idx] - s.remaining)
                        .sum::<ResourceAmount>(),
                )
            })
        {
            result.push(group_idx);
            break;
        } else {
            let Some(group_idx) = (0..n_groups)
                .filter(|group_idx| {
                    states
                        .iter()
                        .any(|s| !s.group_amounts[*group_idx].is_zero() && !s.remaining.is_zero())
                })
                .min_by_key(|group_idx| {
                    (
                        Reverse(
                            states
                                .iter()
                                .map(|s| s.group_amounts[*group_idx].min(s.remaining))
                                .sum::<ResourceAmount>(),
                        ),
                        states
                            .iter()
                            .map(|s| s.group_amounts[*group_idx].saturating_sub(s.remaining))
                            .min()
                            .unwrap_or(ResourceAmount::ZERO),
                        states
                            .iter()
                            .map(|s| s.group_amounts[*group_idx].saturating_sub(s.remaining))
                            .sum::<ResourceAmount>(),
                    )
                })
            else {
                return None;
            };
            for state in states.iter_mut() {
                let (r_units, r_fractions) = state.remaining.split();
                let amount = &mut state.group_amounts[group_idx];
                let (a_units, a_fractions) = amount.split();
                let t_units = r_units.min(a_units);
                let t_fractions = if a_fractions < r_fractions {
                    0
                } else {
                    r_fractions
                };
                let target = ResourceAmount::new(t_units, t_fractions);
                state.remaining -= target;
                *amount = ResourceAmount::ZERO;
            }
            result.push(group_idx);
        }
    }
    Some(result)
}

pub fn find_coupled_groups(
    n_groups: usize,
    free: &ConciseFreeResources,
    entries: &[&crate::resources::ResourceRequestEntry],
) -> Option<GroupSet> {
    let mut states = entries
        .iter()
        .map(|e| {
            let remaining = e.request.amount_or_none_if_all().unwrap();
            GroupMinimizationState {
                remaining,
                group_amounts: free.get(e.resource_id).amount_max_per_group().collect(),
            }
        })
        .collect::<SmallVec<[GroupMinimizationState; FAST_MAX_COUPLED_RESOURCES]>>();
    group_minimizer(n_groups, &mut states)
}

pub fn find_compact_groups(
    n_groups: usize,
    free: &ConciseResourceState,
    entry: &crate::resources::ResourceRequestEntry,
) -> Option<GroupSet> {
    let remaining = entry.request.amount_or_none_if_all().unwrap();
    let mut states = [GroupMinimizationState {
        remaining,
        group_amounts: free.amount_max_per_group().collect(),
    }];
    group_minimizer(n_groups, &mut states)
}
