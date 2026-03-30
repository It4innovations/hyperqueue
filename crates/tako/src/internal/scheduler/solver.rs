use crate::internal::common::resources::{ResourceId, ResourceRequest};
use crate::internal::scheduler::TaskBatch;
use crate::internal::server::core::{Core, CoreSplit};
use crate::internal::server::worker::Worker;
use crate::internal::solver::{ConstraintType, LpSolution, LpSolver, Variable};
use crate::resources::{CPU_RESOURCE_ID, ResourceRqId};
use crate::{Map, ResourceVariantId, Set, WorkerId};
use thin_vec::ThinVec;

#[derive(Default, Debug)]
pub(crate) struct SchedulingSolution {
    pub(crate) sn_counts: Map<(ResourceRqId, ResourceVariantId), Map<WorkerId, u32>>,
    pub(crate) mn_workers: Map<(ResourceRqId, ResourceVariantId), Vec<ThinVec<WorkerId>>>,
}

pub(crate) fn run_scheduling_solver(
    core: &Core,
    now: std::time::Instant,
    task_batches: &[TaskBatch],
    custom_workers: Option<&[Worker]>,
) -> SchedulingSolution {
    let n_resources = core.resource_map().n_resources();

    let CoreSplit {
        task_map: _,
        worker_map,
        task_queues: _,
        request_map,
        worker_groups,
        scheduler_state: scheduler_cache,
        ..
    } = core.split();
    if request_map.is_empty() {
        return SchedulingSolution::default();
    }
    let mut resource_sums = vec![0f64; n_resources];
    let workers: Vec<&Worker> = if let Some(ws) = custom_workers {
        ws.iter().collect()
    } else {
        let mut ws = worker_map
            .get_workers()
            .filter(|w| w.sn_assignment().is_some())
            .collect::<Vec<_>>();
        ws.sort_unstable_by_key(|w| w.id);
        ws
    };

    workers.iter().for_each(|worker| {
        let Some(a) = worker.sn_assignment() else {
            unreachable!()
        };
        a.free_resources
            .iter_amounts()
            .zip(resource_sums.iter_mut())
            .for_each(|(c, s)| {
                if !c.is_max() {
                    *s += c.as_f64()
                } else {
                    *s += 1.0;
                }
            })
    });

    let n_workers = workers.len();

    let mut solver = LpSolver::new(false);

    let mut placements: Map<(WorkerId, ResourceRqId, ResourceVariantId), (_, u32)> = Map::new();
    let mut tasks_count_vars: Map<ResourceRqId, Vec<_>> = Map::new();

    let mut worker_res_constraint = vec![Vec::new(); n_resources];
    let mut worker_cpu_constraint_no_reserves: Vec<(Variable, f64)> =
        Vec::with_capacity(task_batches.len());
    // Create worker-task placements
    for (w_idx, worker) in workers.iter().enumerate() {
        worker_cpu_constraint_no_reserves.clear();
        for batch in task_batches.iter() {
            let rqv = request_map.get(batch.resource_rq_id);
            let mut has_variant = false;
            for (v_idx, rq) in rqv.requests_with_ids() {
                if rq.is_multi_node() {
                    if worker.is_free()
                        && worker_groups
                            .get(&worker.configuration.group)
                            .unwrap()
                            .is_capable_to_run_rq(rq, now, worker_map)
                    {
                        set_placement_name(&mut solver, worker.id, batch.resource_rq_id, v_idx);
                        let v = create_mn_var(
                            &mut solver,
                            rq,
                            n_workers,
                            w_idx,
                            worker,
                            &resource_sums,
                        );
                        placements.insert(
                            (worker.id, batch.resource_rq_id, v_idx),
                            (v, solver.last_var_idx()),
                        );
                        // Insert into worker resource constraints
                        for (r, amount) in worker.resources.iter_pairs() {
                            worker_res_constraint[r.as_usize()].push((v, amount.as_f64()));
                        }
                    }
                } else if !worker.is_request_blocked(batch.resource_rq_id, v_idx)
                    && worker.has_time_to_run(rq.min_time(), now)
                    && worker.have_immediate_resources_for_rq(rq)
                {
                    has_variant = true;
                    set_placement_name(&mut solver, worker.id, batch.resource_rq_id, v_idx);
                    let v =
                        create_sn_var(&mut solver, rq, n_workers, w_idx, worker, &resource_sums);
                    placements.insert(
                        (worker.id, batch.resource_rq_id, v_idx),
                        (v, solver.last_var_idx()),
                    );
                    tasks_count_vars
                        .entry(batch.resource_rq_id)
                        .or_default()
                        .push(v);

                    // Insert into worker resource constraints
                    for e in rq.entries() {
                        let r = e.resource_id;
                        let amount = e
                            .request
                            .amount_or_none_if_all()
                            .unwrap_or_else(|| worker.resources.get(r))
                            .as_f64();
                        worker_res_constraint[r.as_usize()].push((v, amount));
                        if r == CPU_RESOURCE_ID {
                            worker_cpu_constraint_no_reserves.push((v, amount));
                        }
                    }
                }
            }

            // If possible, create a reservation variable
            if !has_variant
                && !rqv.is_multi_node()
                && !batch.limit_reached
                && batch.is_blocker
                && worker.is_capable_to_run_rqv(rqv, now)
                && let Some(a) = worker.sn_assignment()
            {
                let weight = w_idx as f64 / (n_workers * 100) as f64;
                solver.set_name(|| format!("R{}:{}", worker.id, batch.resource_rq_id));
                let v = solver.add_bool_variable(weight);
                tasks_count_vars
                    .entry(batch.resource_rq_id)
                    .or_default()
                    .push(v);
                for (res_id, count) in a.free_resources.iter_pairs() {
                    worker_res_constraint[res_id.as_usize()].push((v, count.as_f64()));
                }
            }
        }

        if worker.configuration.min_utilization > 0.001 {
            add_min_utilization(&mut solver, worker, &mut worker_cpu_constraint_no_reserves);
        }

        // Create worker constraints
        for (r, c) in worker_res_constraint.iter_mut().enumerate() {
            let free = worker
                .sn_assignment()
                .unwrap()
                .free_resources
                .get(ResourceId::new(r as u32));
            if free.is_max() {
                continue;
            }
            if !c.is_empty() {
                solver.set_name(|| format!("w{} resource limit", worker.id));
                solver.add_constraint(ConstraintType::Max, free.as_f64(), c.iter().copied())
            }
            c.clear();
        }
    }
    let mut task_counts_per_group: Map<(ResourceRqId, &str), Variable> = Map::new();
    let mut temp = Vec::new();
    for batch in task_batches.iter() {
        let batch_rqv = request_map.get(batch.resource_rq_id);
        if batch_rqv.is_multi_node() {
            let n_nodes = batch_rqv.unwrap_first().n_nodes() as f64;
            let rv_id = ResourceVariantId::new(0);
            for (group_name, group) in worker_groups.iter() {
                temp.clear();
                for w_id in group.worker_ids() {
                    if let Some((v, _)) = placements.get(&(w_id, batch.resource_rq_id, rv_id)) {
                        temp.push(*v)
                    }
                }
                if !temp.is_empty() {
                    solver.set_name(|| format!("mn_{}_{}", batch.resource_rq_id, group_name));
                    let v = solver.add_nat_variable(0.0);
                    solver.set_name(|| format!("MN size for rq{}", batch.resource_rq_id));
                    constraint_extra_var(&mut solver, ConstraintType::Eq, 0.0, &temp, v, -n_nodes);
                    tasks_count_vars
                        .entry(batch.resource_rq_id)
                        .or_default()
                        .push(v);
                    task_counts_per_group.insert((batch.resource_rq_id, group_name), v);
                }
            }
        }
    }

    // blocking_variable_vars[(rq_id, s)] is True only if there is at least
    // `s` tasks of `rq_id` scheduled
    let mut blocked_priority_vars: Map<(ResourceRqId, u32), _> = Map::new();

    let mut get_bvar = |solver: &mut LpSolver, blocker_rq_id: ResourceRqId, size: u32| {
        *blocked_priority_vars
            .entry((blocker_rq_id, size))
            .or_insert_with(|| {
                // Create a new blocking variable
                solver.set_name(|| format!("B{}~{}", blocker_rq_id, size));
                let new_v = solver.add_bool_variable(0.0);
                let vars = tasks_count_vars.get(&blocker_rq_id).unwrap();
                solver.set_name(|| format!("blocker rq{blocker_rq_id} at size {size}"));
                let bound = size as f64;
                constraint_extra_var(solver, ConstraintType::Min, bound, vars, new_v, bound);
                new_v
            })
    };

    let mut zero_cond = Vec::new();
    let mut blocked_by_unbounded: Set<ResourceRqId> = Set::new();

    for batch in task_batches.iter() {
        let Some(task_counts) = tasks_count_vars.get(&batch.resource_rq_id) else {
            continue;
        };
        let batch_rqv = request_map.get(batch.resource_rq_id);
        assert!(!task_counts.is_empty());
        if !batch.limit_reached {
            solver.set_name(|| format!("size limit for rq{}", batch.resource_rq_id));
            solver.add_constraint(
                ConstraintType::Max,
                batch.size as f64,
                task_counts.iter().map(|v| (*v, 1.0)),
            )
        }
        let batch_size = batch.size as f64;
        blocked_by_unbounded.clear();
        for cut in &batch.cuts {
            for (blocker_rq_id, blocking_size) in &cut.blockers {
                zero_cond.clear();
                let blocker_rqv = request_map.get(*blocker_rq_id);
                if batch_rqv.is_multi_node() {
                    for (group_name, group) in worker_groups.iter() {
                        if let Some(v) =
                            task_counts_per_group.get(&(batch.resource_rq_id, group_name.as_str()))
                            && group.is_capable_to_run(blocker_rqv, now, worker_map)
                        {
                            zero_cond.push(*v);
                        }
                    }
                } else {
                    for w in &workers {
                        if !w.is_capable_to_run_rqv(blocker_rqv, now) {
                            continue;
                        }
                        for v_id in batch_rqv.variant_ids() {
                            if let Some((var, _)) =
                                placements.get(&(w.id, batch.resource_rq_id, v_id))
                            {
                                let gap = scheduler_cache.gap_cache.get_gap(
                                    *blocker_rq_id,
                                    batch.resource_rq_id,
                                    v_id,
                                    &w.resources,
                                    request_map,
                                );
                                if gap > 0 {
                                    let cut_size = cut.size as f64;
                                    if let Some(s) = blocking_size {
                                        let blocking_v = get_bvar(&mut solver, *blocker_rq_id, *s);
                                        solver.set_name(|| {
                                            format!(
                                                "w{}: if #rq{blocker_rq_id} < {s} then limit #rq{} to {} + {} (gap) where both rqs may run",
                                                w.id, batch.resource_rq_id, cut.size, gap
                                            )
                                        });
                                        solver.add_constraint(
                                            ConstraintType::Max,
                                            cut_size + batch_size + gap as f64,
                                            [(*var, 1.0), (blocking_v, batch_size)].into_iter(),
                                        );
                                    } else {
                                        solver.set_name(|| {
                                            format!(
                                                "w{}: limit #rq{} to {} + {} (gap) where it can run with rq{blocker_rq_id}",
                                                w.id, batch.resource_rq_id, gap, cut.size,
                                            )
                                        });
                                        solver.add_constraint(
                                            ConstraintType::Max,
                                            cut_size + gap as f64,
                                            [(*var, 1.0)].into_iter(),
                                        );
                                    }
                                } else {
                                    zero_cond.push(*var);
                                }
                            }
                        }
                    }
                }
                if zero_cond.is_empty() {
                    continue;
                }
                if let Some(s) = blocking_size {
                    let blocking_v = get_bvar(&mut solver, *blocker_rq_id, *s);
                    solver.set_name(|| {
                        format!(
                            "if #rq{blocker_rq_id} < {s} then limit #rq{} to {} where both rqs may run",
                            batch.resource_rq_id, cut.size
                        )
                    });
                    let cut_size = cut.size as f64;
                    constraint_extra_var(
                        &mut solver,
                        ConstraintType::Max,
                        batch_size + cut_size,
                        &zero_cond,
                        blocking_v,
                        batch_size,
                    );
                } else if !blocked_by_unbounded.contains(blocker_rq_id) {
                    blocked_by_unbounded.insert(*blocker_rq_id);
                    solver.set_name(|| {
                        format!(
                            "limit #rq{} to {} where it can run with rq{blocker_rq_id}",
                            batch.resource_rq_id, cut.size
                        )
                    });
                    solver.add_constraint(
                        ConstraintType::Max,
                        cut.size as f64,
                        zero_cond.iter().map(|v| (*v, 1.0)),
                    );
                }
            }
        }
    }

    let mut result = SchedulingSolution::default();
    let Some((solution, _)) = solver.solve() else {
        return result;
    };

    let values = solution.get_values();
    for batch in task_batches {
        let resource_rq_id = batch.resource_rq_id;
        let rqv = request_map.get(resource_rq_id);
        if rqv.is_multi_node() {
            let v_id = ResourceVariantId::new(0);
            let n_nodes = rqv.get(v_id).n_nodes() as usize;
            let mut ws: Vec<ThinVec<WorkerId>> = Vec::new();
            for worker in &workers {
                if let Some((_, var_idx)) = placements.get(&(worker.id, resource_rq_id, v_id)) {
                    let count = values[*var_idx as usize].round() as u32;
                    if count > 0 {
                        if let Some(last) = ws.last_mut()
                            && last.len() < n_nodes
                        {
                            last.push(worker.id);
                        } else {
                            let mut workers = ThinVec::with_capacity(n_nodes);
                            workers.push(worker.id);
                            ws.push(workers);
                        }
                    }
                }
            }
            if !ws.is_empty() {
                result.mn_workers.insert((resource_rq_id, v_id), ws);
            }
        } else {
            for v_id in rqv.variant_ids() {
                let counts: Map<_, _> = workers
                    .iter()
                    .filter_map(|w| {
                        placements
                            .get(&(w.id, resource_rq_id, v_id))
                            .and_then(|(_, var_idx)| {
                                let count = values[*var_idx as usize].round() as u32;
                                if count > 0 {
                                    Some((w.id, values[*var_idx as usize].round() as u32))
                                } else {
                                    None
                                }
                            })
                    })
                    .collect();
                if !counts.is_empty() {
                    result.sn_counts.insert((resource_rq_id, v_id), counts);
                }
            }
        }
    }
    result
}

fn set_placement_name(
    solver: &mut LpSolver,
    worker_id: WorkerId,
    resource_rq_id: ResourceRqId,
    v_idx: ResourceVariantId,
) {
    solver.set_name(|| {
        let mut s = format!("w{}:r{}", worker_id, resource_rq_id);
        if v_idx.is_first() {
            use std::fmt::Write;
            write!(&mut s, ":{}", v_idx).unwrap();
        }
        s
    });
}

fn add_min_utilization(
    solver: &mut LpSolver,
    worker: &Worker,
    worker_res_constraint: &mut Vec<(Variable, f64)>,
) {
    let Some(sn) = worker.sn_assignment() else {
        return;
    };
    let free_cpus = sn.free_resources.get(CPU_RESOURCE_ID).as_f64();
    let all_cpus = worker.resources.get(CPU_RESOURCE_ID).as_f64();
    // Explanation: min_cpus = mu * all - used = mu * all - (all - free) = (mu - 1) * all + free
    let min_cpus = all_cpus * (worker.configuration.min_utilization as f64 - 1.0) + free_cpus;
    if min_cpus < 0.0001 {
        return;
    }
    solver.set_name(|| format!("mu_{}", worker.id));
    let v = solver.add_bool_variable(0.0);
    worker_res_constraint.push((v, -min_cpus));
    solver.set_name(|| format!("w{} min utilization (lower bound)", worker.id));
    solver.add_constraint(
        ConstraintType::Min,
        0.0,
        worker_res_constraint.iter().copied(),
    );
    worker_res_constraint.pop();
    solver.set_name(|| format!("w{} min utilization (upper bound)", worker.id));
    worker_res_constraint.push((v, -all_cpus));
    solver.add_constraint(
        ConstraintType::Max,
        0.0,
        worker_res_constraint.iter().copied(),
    );
    worker_res_constraint.pop();
}

fn create_sn_var(
    solver: &mut LpSolver,
    rq: &ResourceRequest,
    n_workers: usize,
    w_idx: usize,
    worker: &Worker,
    resource_sums: &[f64],
) -> Variable {
    let weight = rq
        .entries()
        .iter()
        .map(|e| {
            let r = e.resource_id;
            let global = resource_sums[r.as_usize()];
            if global < 0.000001 {
                return 0.0;
            }
            e.request
                .amount_or_none_if_all()
                .unwrap_or_else(|| worker.resources.get(r))
                .as_f64()
                / global
        })
        .sum::<f64>()
        * (n_workers - w_idx) as f64
        * rq.weight().as_f64()
        / n_workers as f64;

    solver.add_nat_variable(weight)
}

fn create_mn_var(
    solver: &mut LpSolver,
    rq: &ResourceRequest,
    n_workers: usize,
    w_idx: usize,
    worker: &Worker,
    resource_sums: &[f64],
) -> Variable {
    let weight = worker
        .resources
        .iter_pairs()
        .map(|(r, amount)| {
            let global = resource_sums[r.as_usize()];
            if global < 0.000001 {
                return 0.0;
            }
            amount.as_f64() / global
        })
        .sum::<f64>()
        * (n_workers - w_idx) as f64
        * rq.weight().as_f64()
        / n_workers as f64;

    solver.add_bool_variable(weight)
}

fn constraint_extra_var(
    solver: &mut LpSolver,
    constraint_type: ConstraintType,
    limit_value: f64,
    vars: &[Variable],
    var: Variable,
    coef: f64,
) {
    solver.add_constraint(
        constraint_type,
        limit_value,
        vars.iter()
            .map(|v| (*v, 1.0))
            .chain(std::iter::once((var, coef))),
    );
}
