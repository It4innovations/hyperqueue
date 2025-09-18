use crate::internal::common::resources::ResourceId;
use crate::internal::worker::resources::concise::ConciseFreeResources;
use crate::internal::worker::resources::pool::{FAST_MAX_COUPLED_RESOURCES, FAST_MAX_GROUPS};
use crate::internal::worker::resources::solver::create_solver;
use crate::internal::worker::resources::solver::{LpSolution, LpSolver};
use crate::resources::{FRACTIONS_PER_UNIT, ResourceGroupIdx};
use smallvec::SmallVec;

pub(crate) struct CouplingWeightItem {
    pub(crate) resource1: ResourceId,
    pub(crate) group1: ResourceGroupIdx,
    pub(crate) resource2: ResourceId,
    pub(crate) group2: ResourceGroupIdx,
    pub(crate) weight: f64,
}

type GroupIndices = SmallVec<[usize; 2]>;
type SelectedGroups = SmallVec<[GroupIndices; FAST_MAX_COUPLED_RESOURCES]>;

/*
   This is the main solver for the NUMA aware scheduling. It find the optimal solution
   wrt minimizing the total number of groups & respecting weights between groups.

   Note that the solver does not select specific indices nor the number of indices that would be taken
   from each group. It is done later and depends on the allocation strategy. Here we just find
   which groups we will consider later.

   It solves the following MLP problem:

    Consts:
    * f_i_j = number of whole free indices in resource group j of resource i
    * g_i_j = number of biggest free fraction resources indices in resource group j of resource i
    * w_i1_ji_i2_j2 = weight between resource group j1 of resource i1 and group j2 of resource i2
    * r_i = number of whole indices in request for resource i
    * z_i = fractional part of request for resource i

    Variables:
    * v_i_j = 1 iff group j of resources i has to chosen; 0 means that is not chosen
    * u_i1_ji_i2_j2 exists if there is weight w_i1_ji_i2_j2 and both v_i1_j1 is and v_i2_j2 is set to 1.

    Conditions:

    # Cannot se u to 1 if both connected vs are not selected.
    u_i1_ji_i2_j2 <= v_i1_j1
    u_i1_ji_i2_j2 <= v_i1_j1

    # Selected groups have enough whole resources
    f_i_0 * u_i_0 + f_i_1 * u_i_1 ... >= r_i

    # Selected groups have enough fractional resources
    # It is tricky here as we want to select fractional resources from only a single group
    if z_i > 0:
        (f_i_0 + (1 if z_i < g_i_0 else 0)) * u_i_0 + (f_i_1 + (1 if z_i < g_i_1 else 0) * u_i_1 ... >= r_i + 1

    Optimized expression (maximization)

    for valid i, j: (-1024 + (g_i_j * 16 if z_i < g_i_j else 0) * v_i_j
    + for each valid i1,j2,i2,j2: w_i1_ji_i2_j2 * u_i1_ji_i2_j2 ...

    (Note:  (g_i_j * 16 if z_i < g_i_j else 0) serves to select the biggest free fraction if available))
*/
pub fn group_solver(
    free: &ConciseFreeResources,
    entries: &[&crate::resources::ResourceAllocRequest],
    weights: &[CouplingWeightItem],
) -> Option<(SelectedGroups, f64)> {
    let mut solver = create_solver();
    let vars: SmallVec<[SmallVec<_>; FAST_MAX_COUPLED_RESOURCES]> = entries
        .iter()
        .map(|entry| {
            let (units, fractions) = entry.request.amount_or_none_if_all().unwrap().split();
            let r = free.get(entry.resource_id);
            if fractions == 0 {
                let vs = r
                    .units_per_group()
                    .map(|u| solver.add_bool_variable(-1024.0 - (u as f64) / 32.0))
                    .collect::<SmallVec<[_; FAST_MAX_GROUPS]>>();
                solver.add_constraint(
                    units as f64,
                    vs.iter()
                        .zip(r.units_per_group())
                        .map(|(v, u)| (*v, u as f64)),
                );
                vs
            } else {
                let amounts: SmallVec<[_; FAST_MAX_GROUPS]> = r.amount_max_per_group().collect();
                let mut need_second_check = false;
                let vs: SmallVec<[_; FAST_MAX_GROUPS]> = amounts
                    .iter()
                    .map(|(_, f)| {
                        if *f >= fractions {
                            need_second_check = true;
                            solver.add_bool_variable(
                                -1024.0 + (*f as f64 / (FRACTIONS_PER_UNIT as f64 / 16.0)),
                            )
                        } else {
                            solver.add_bool_variable(-1024.0)
                        }
                    })
                    .collect();
                solver.add_constraint(
                    (units + 1) as f64,
                    vs.iter()
                        .zip(amounts.iter())
                        .map(|(v, (u, f))| (*v, if *f >= fractions { u + 1 } else { *u } as f64)),
                );
                if units > 0 && need_second_check {
                    solver.add_constraint(
                        units as f64,
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
        let v3 = solver.add_variable(w.weight, 0.0, 1.0);
        solver.add_constraint(0.0, [(v1, 1.0), (v3, -1.0)].into_iter());
        solver.add_constraint(0.0, [(v2, 1.0), (v3, -1.0)].into_iter());
    }
    let (solution, objective_value) = solver.solve()?;
    let values = solution.get_values();
    let mut index = 0;
    Some((
        entries
            .iter()
            .map(|entry| {
                let r = free.get(entry.resource_id);
                let n = r.n_groups();
                let g = values[index..index + n]
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
