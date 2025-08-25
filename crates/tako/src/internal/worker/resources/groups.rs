use crate::internal::common::resources::ResourceId;
use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
use crate::internal::worker::resources::pool::{FAST_MAX_COUPLED_RESOURCES, FAST_MAX_GROUPS};
use crate::resources::{AllocationRequest, ResourceAmount, ResourceGroupIdx, FRACTIONS_PER_UNIT};
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

type GroupIndices = SmallVec<[usize; 2]>;
type SelectedGroups = SmallVec<[GroupIndices; FAST_MAX_COUPLED_RESOURCES]>;

pub fn group_solver(
    free: &ConciseFreeResources,
    entries: &[&crate::resources::ResourceAllocRequest],
    weights: &[CouplingWeightItem],
) -> Option<(SelectedGroups, f64)> {
    let mut pb = highs::RowProblem::new();
    let vars: SmallVec<[SmallVec<_>; FAST_MAX_COUPLED_RESOURCES]> = entries
        .iter()
        .map(|entry| {
            let (units, fractions) = entry.request.amount_or_none_if_all().unwrap().split();
            let r = free.get(entry.resource_id);
            if fractions == 0 {
                let vs = (0..r.n_groups())
                    .map(|_| pb.add_integer_column(-1024.0, 0..=1))
                    .collect::<SmallVec<[highs::Col; FAST_MAX_GROUPS]>>();
                pb.add_row(
                    units..,
                    vs.iter()
                        .zip(r.units_per_group())
                        .map(|(v, u)| (*v, u as f64)),
                );
                vs
            } else {
                let amounts: SmallVec<[_; FAST_MAX_GROUPS]> = r.amount_max_per_group().collect();
                let mut need_second_check = false;
                let vs: SmallVec<[highs::Col; FAST_MAX_GROUPS]> = amounts
                    .iter()
                    .map(|(_, f)| {
                        if *f >= fractions {
                            need_second_check = true;
                            pb.add_integer_column(
                                -1024.0 + (*f as f64 / (FRACTIONS_PER_UNIT as f64 / 16.0)),
                                0..=1,
                            )
                        } else {
                            pb.add_integer_column(-1024.0, 0..=1)
                        }
                    })
                    .collect();
                pb.add_row(
                    (units + 1)..,
                    vs.iter()
                        .zip(amounts.iter())
                        .map(|(v, (u, f))| (*v, if *f >= fractions { u + 1 } else { *u } as f64)),
                );
                if units > 0 && need_second_check {
                    pb.add_row(
                        units..,
                        vs.iter()
                            .zip(r.units_per_group())
                            .map(|(v, u)| (*v, u as f64)),
                    );
                }
                vs
            }
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
        let v3 = pb.add_column(w.weight, 0..=1);
        pb.add_row(0.., &[(v1, 1.0), (v3, -1.0)]);
        pb.add_row(0.., &[(v2, 1.0), (v3, -1.0)]);
    }

    let solved_model = pb.optimise(Sense::Maximise).solve();
    if !matches!(solved_model.status(), HighsModelStatus::Optimal) {
        return None;
    }
    let objective_value = solved_model.objective_value();
    let solution = solved_model.get_solution();
    let columns = solution.columns();
    let mut index = 0;

    Some((
        entries
            .iter()
            .map(|entry| {
                let r = free.get(entry.resource_id);
                let n = r.n_groups();
                let g = (&columns[index..index + n])
                    .iter()
                    .enumerate()
                    .filter_map(|(i, v)| (*v > 0.5).then_some(i))
                    .collect();
                index += n;
                g
            })
            .collect(),
        objective_value,
    ))
}
