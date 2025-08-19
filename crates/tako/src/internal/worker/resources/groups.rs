use crate::internal::common::resources::ResourceId;
use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
use crate::internal::worker::resources::pool::{FAST_MAX_COUPLED_RESOURCES, FAST_MAX_GROUPS};
use crate::resources::{AllocationRequest, ResourceAmount, ResourceGroupIdx};
use highs::{HighsModelStatus, Sense};
use smallvec::SmallVec;

pub(crate) struct CouplingWeightItem {
    pub(crate) resource1: ResourceId,
    pub(crate) group1: ResourceGroupIdx,
    pub(crate) resource2: ResourceId,
    pub(crate) group2: ResourceGroupIdx,
    pub(crate) weight: f64,
}

struct GroupMinimizationRequest {
    request: ResourceAmount,
    group_amounts: SmallVec<[ResourceAmount; FAST_MAX_GROUPS]>,
    resource_id: ResourceId,
}

type GroupSet = SmallVec<[usize; 2]>;

fn group_solver(
    free: &ConciseFreeResources,
    entries: &[&crate::resources::ResourceAllocRequest],
    weights: &[CouplingWeightItem],
) -> Option<GroupSet> {
    let mut pb = highs::RowProblem::new();
    let vars: SmallVec<[SmallVec<_>; FAST_MAX_GROUPS]> = entries
        .iter()
        .map(|entry| {
            let (units, fractions) = entry.request.amount_or_none_if_all().unwrap().split();
            assert_eq!(fractions, 0);
            let r = free.get(entry.resource_id);
            let vs: SmallVec<[highs::Col; FAST_MAX_COUPLED_RESOURCES]> = (0..r.n_groups())
                .map(|_| {
                    let var = pb.add_integer_column(-1024.0, 0..=1);
                    var
                })
                .collect();
            pb.add_row(
                units..,
                vs.iter().zip(r.amount_max_per_group()).map(|(v, a)| {
                    let (u, f) = a.split();
                    assert_eq!(f, 0);
                    (*v, u as f64)
                }),
            );
            vs
        })
        .collect();
    for w in weights {
        let Some(r1) = entries.iter().position(|e| e.resource_id == w.resource1) else {
            continue;
        };
        let Some(r2) = entries.iter().position(|e| e.resource_id == w.resource2) else {
            continue;
        };
        let v1 = vars[r1][w.group1.as_num() as usize];
        let v2 = vars[r2][w.group2.as_num() as usize];
        let v3 = pb.add_integer_column(w.weight, 0..=1);
        pb.add_row(0.., &[(v1, 1.0), (v3, -1.0)]);
        pb.add_row(0.., &[(v2, 1.0), (v3, -1.0)]);
    }

    let solved_model = pb.optimise(Sense::Maximise).solve();
    if !matches!(solved_model.status(), HighsModelStatus::Optimal) {
        return None;
    }
    let solution = solved_model.get_solution().columns();

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
            GroupMinimizationRequest {
                request: remaining,
                group_amounts: free.get(e.resource_id).amount_max_per_group().collect(),
            }
        })
        .collect::<SmallVec<[GroupMinimizationRequest; FAST_MAX_COUPLED_RESOURCES]>>();
    todo!()
    //group_solver(n_groups, &mut states)
}

pub fn find_compact_groups(
    n_groups: usize,
    free: &ConciseResourceState,
    policy: &AllocationRequest,
) -> Option<GroupSet> {
    let remaining = policy.amount_or_none_if_all().unwrap();
    let mut states = [GroupMinimizationRequest {
        request: remaining,
        group_amounts: free.amount_max_per_group().collect(),
    }];
    todo!();
    //group_solver(n_groups, &mut states)
}
