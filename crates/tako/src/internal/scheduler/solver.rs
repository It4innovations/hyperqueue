use crate::internal::common::resources::{
    ResourceAmount, ResourceId, ResourceRequest, ResourceRequestVariants,
};
use crate::internal::scheduler::TaskBatch;
use crate::internal::scheduler::state::SchedulerState;
use crate::internal::server::core::{Core, CoreSplit};
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::worker::Worker;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::solver::{ConstraintType, LpSolution, LpSolver, Variable};
use crate::resources::{CPU_RESOURCE_ID, ResourceRqId, ResourceRqMap};
use crate::{Map, ResourceVariantId, Set, WorkerId};
use thin_vec::ThinVec;

#[derive(Debug)]
pub(crate) struct SchedulingSolution {
    pub(crate) sn_counts: Map<(ResourceRqId, ResourceVariantId), Map<WorkerId, u32>>,
    pub(crate) mn_workers: Map<(ResourceRqId, ResourceVariantId), Vec<ThinVec<WorkerId>>>,
    /// `false` if the MILP solve hit its time limit before proving
    /// optimality (see `SchedulerConfig::mip_time_limit`).
    pub(crate) is_optimal: bool,
}

impl Default for SchedulingSolution {
    fn default() -> Self {
        SchedulingSolution {
            sn_counts: Map::new(),
            mn_workers: Map::new(),
            is_optimal: true,
        }
    }
}

impl SchedulingSolution {
    pub fn is_empty(&self) -> bool {
        self.sn_counts.values().all(|v| v.is_empty())
            && self.mn_workers.values().all(|v| v.is_empty())
    }
}

pub(crate) fn run_scheduling_solver(
    core: &Core,
    now: std::time::Instant,
    task_batches: &[TaskBatch],
    custom_workers: Option<&[Worker]>,
) -> SchedulingSolution {
    let n_resources = core.resource_map().n_resources();

    let CoreSplit {
        task_map,
        worker_map,
        task_queues: _,
        request_map,
        worker_groups,
        scheduler_state,
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

    let mut placements: Map<(WorkerId, ResourceRqId, ResourceVariantId), Variable> = Map::new();
    let mut tasks_count_vars: Map<ResourceRqId, Vec<_>> = Map::new();
    // Reservation variables by (worker, request). A reservation stands for one task of its request
    // set aside on that worker, so it also enters the request's own priority conditions there.
    let mut reservations: Map<(WorkerId, ResourceRqId), Variable> = Map::new();

    let mut worker_res_constraint = vec![Vec::new(); n_resources];
    let mut worker_cpu_constraint_no_reserves: Vec<(Variable, f64)> =
        Vec::with_capacity(task_batches.len());

    // Held workers are chosen before any variable exists, because a reservation variable is only
    // created on a held worker. A reservation elsewhere would count toward its blocker without
    // holding anything back: on a worker with no free resources it costs nothing but its small
    // penalty, and with no worker held (the blocker fits somewhere right now) it would let a
    // lower-priority request take the very capacity the blocker was counted as using.
    let reserved = held_workers(
        &workers,
        task_batches,
        request_map,
        task_map,
        scheduler_state,
        now,
    );
    // The reservation penalty is expressed relative to the placements a reservation can unlock.
    // Placement weights shrink as the cluster's free resources grow, so a fixed penalty would
    // outweigh every placement on a large cluster and no reservation would ever pay for itself.
    let reservation_scale =
        placement_weight_lower_bound(&workers, task_batches, request_map, &resource_sums);

    // Create worker-task placements
    let mut worker_reservations: Vec<Variable> = Vec::new();
    for (w_idx, worker) in workers.iter().enumerate() {
        worker_cpu_constraint_no_reserves.clear();
        worker_reservations.clear();
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
                        placements.insert((worker.id, batch.resource_rq_id, v_idx), v);
                        // Insert into worker resource constraints
                        for (r, amount) in worker.resources.iter_nonzero_pairs() {
                            worker_res_constraint[r.as_usize()].push((v, amount.as_f64()));
                        }
                    }
                } else if sn_variant_fits_now(worker, batch.resource_rq_id, v_idx, rq, now) {
                    has_variant = true;
                    set_placement_name(&mut solver, worker.id, batch.resource_rq_id, v_idx);
                    let v =
                        create_sn_var(&mut solver, rq, n_workers, w_idx, worker, &resource_sums);
                    placements.insert((worker.id, batch.resource_rq_id, v_idx), v);
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

            if !has_variant
                && reserved
                    .get(&batch.resource_rq_id)
                    .is_some_and(|held| held.order.contains(&worker.id))
                && let Some(a) = worker.sn_assignment()
            {
                let weight =
                    -reservation_scale * (n_workers - w_idx) as f64 / (n_workers * 1024) as f64;
                solver.set_name(|| format!("R{}:{}", worker.id, batch.resource_rq_id));
                let v = solver.add_bool_variable(weight);
                reservations.insert((worker.id, batch.resource_rq_id), v);
                worker_reservations.push(v);
                tasks_count_vars
                    .entry(batch.resource_rq_id)
                    .or_default()
                    .push(v);
                // A reservation withholds the free resources the blocker could use, but not its gap
                // allowance: that capacity is lost to the blocker anyway, and the priority
                // conditions already let lower-priority gap tasks use it.
                let gap = worker_gap(
                    worker,
                    batch.resource_rq_id,
                    request_map,
                    task_map,
                    scheduler_state,
                );
                for (res_id, count) in a.free_resources.iter_nonzero_pairs() {
                    let withheld = gap
                        .as_ref()
                        .map_or(count, |g| count.saturating_sub(g.get(res_id)));
                    if !withheld.is_zero() {
                        worker_res_constraint[res_id.as_usize()].push((v, withheld.as_f64()));
                    }
                }
            }
        }

        // A worker can later host only one of the blockers it is reserved for, so it may serve
        // at most one. The resource constraints do not ensure this: on a worker with no free
        // resources a reservation consumes nothing, and any number of them would fit.
        if worker_reservations.len() > 1 {
            solver.set_name(|| format!("w{}: at most one reservation", worker.id));
            solver.add_constraint(
                ConstraintType::Max,
                1.0,
                worker_reservations.iter().map(|v| (*v, 1.0)),
            );
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
                    if let Some(v) = placements.get(&(w_id, batch.resource_rq_id, rv_id)) {
                        temp.push(*v)
                    }
                }
                if !temp.is_empty() {
                    solver.set_name(|| format!("mn_{}_{}", batch.resource_rq_id, group_name));
                    let v = solver.add_nat_variable(0.0);
                    solver.set_name(|| format!("MN size for rq{}", batch.resource_rq_id));
                    constraint_extra_var(
                        &mut solver,
                        ConstraintType::Eq,
                        0.0,
                        temp.iter().copied(),
                        v,
                        -n_nodes,
                    );
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
        if let Some(v) = blocked_priority_vars.get(&(blocker_rq_id, size)) {
            return Some(*v);
        }
        let vars = tasks_count_vars.get(&blocker_rq_id)?;
        // Create a new blocking variable
        solver.set_name(|| format!("B{}~{}", blocker_rq_id, size));
        let new_v = solver.add_bool_variable(0.0);
        solver.set_name(|| format!("blocker rq{blocker_rq_id} at size {size}"));
        let bound = size as f64;
        constraint_extra_var(
            solver,
            ConstraintType::Min,
            bound,
            vars.iter().copied(),
            new_v,
            bound,
        );
        blocked_priority_vars.insert((blocker_rq_id, size), new_v);
        Some(new_v)
    };

    // Terms of a condition's left-hand side: every placement, minus what sits in a worker's gap.
    let mut cond_terms: Vec<(Variable, f64)> = Vec::new();
    let mut cond_terms_reserved: Vec<(Variable, f64)> = Vec::new();
    let mut blocked_by_unbounded: Set<ResourceRqId> = Set::new();
    // Gap parts per (worker, blocker, resource): the gap allowance and the terms that share it.
    let mut shared_gap: Map<SharedGapKey, (ResourceAmount, Vec<(Variable, f64)>)> = Map::new();
    let mut shared_gap_seen: Set<(WorkerId, ResourceRqId, ResourceRqId)> = Set::new();

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
                cond_terms.clear();
                cond_terms_reserved.clear();
                let blocker_rqv = request_map.get(*blocker_rq_id);
                if batch_rqv.is_multi_node() {
                    for (group_name, group) in worker_groups.iter() {
                        if let Some(v) =
                            task_counts_per_group.get(&(batch.resource_rq_id, group_name.as_str()))
                            && group.is_capable_to_run(blocker_rqv, now, worker_map)
                        {
                            cond_terms.push((*v, 1.0));
                        }
                    }
                } else {
                    for w in &workers {
                        let Some(sn_assignment) = w.sn_assignment() else {
                            continue;
                        };
                        if !w.is_capable_to_run_rqv(blocker_rqv, now) {
                            continue;
                        }
                        let gap_resources = scheduler_state.gap_cache.gap_resources(
                            *blocker_rq_id,
                            &w.resources,
                            sn_assignment.assigned_tasks.iter().map(|task_id| {
                                let t = task_map.get_task(*task_id);
                                (
                                    t.resource_rq_id,
                                    t.assigned_placement(&scheduler_state.redirects).unwrap().1,
                                )
                            }),
                            request_map,
                        );
                        let gap = gap_resources
                            .as_ref()
                            .map(|g| gap_count(g, batch_rqv))
                            .unwrap_or(0);
                        // A worker held for this blocker gets the unconditional form: with no
                        // blocker-count term there is nothing a reservation variable can discharge.
                        let is_reserved = blocking_size.is_some_and(|threshold| {
                            reserved
                                .get(blocker_rq_id)
                                .is_some_and(|held| held.holds(w.id, threshold))
                        });
                        // A reservation counts like a placement of its request: while the blocker is
                        // unserved on this worker, the request may not set capacity aside here any
                        // more than it may start a task here.
                        let reservation = reservations.get(&(w.id, batch.resource_rq_id)).copied();
                        let placement_vars = batch_rqv
                            .variant_ids()
                            .filter_map(|v_id| {
                                placements
                                    .get(&(w.id, batch.resource_rq_id, v_id))
                                    .map(|v| (v_id, *v))
                            })
                            .collect::<Vec<_>>();
                        if placement_vars.is_empty() && reservation.is_none() {
                            continue;
                        }
                        let placed = placement_vars
                            .iter()
                            .map(|(_, v)| (*v, 1.0))
                            .chain(reservation.map(|v| (v, 1.0)));
                        cond_terms.extend(placed.clone());
                        if is_reserved {
                            cond_terms_reserved.extend(placed);
                        }
                        if gap == 0 {
                            // Nothing can sit in a gap here, so every task placed counts against
                            // the before-count. No variable and no constraint for this worker.
                            continue;
                        }
                        // Tasks of this request sitting in the worker's gap. They do not count
                        // against the before-count, so they are subtracted from the condition.
                        // At most `gap` of them fit, which is a bound of the variable itself.
                        solver.set_name(|| {
                            format!(
                                "w{}: #rq{} within the {gap} gap of rq{blocker_rq_id}",
                                w.id, batch.resource_rq_id
                            )
                        });
                        let gap_var = solver.add_variable(0.0, 0.0, gap as f64);
                        // Only tasks that really run here may sit in the gap. Without this, a
                        // request could claim gap usage it does not have, and so shrink the
                        // condition below and the shared gap constraint for other requests.
                        solver.set_name(|| {
                            format!(
                                "w{}: #rq{} in the gap is at most what runs here",
                                w.id, batch.resource_rq_id
                            )
                        });
                        constraint_extra_var(
                            &mut solver,
                            ConstraintType::Min,
                            0.0,
                            placement_vars.iter().map(|(_, v)| *v).chain(reservation),
                            gap_var,
                            -1.0,
                        );
                        cond_terms.push((gap_var, -1.0));
                        if is_reserved {
                            cond_terms_reserved.push((gap_var, -1.0));
                        }
                        // All requests blocked by this blocker share the worker's gap, so their
                        // gap parts are collected per resource. Recorded once per (worker,
                        // blocker, request): the gap does not depend on the cut.
                        if let Some(gap_resources) = gap_resources.as_ref()
                            && shared_gap_seen.insert((w.id, *blocker_rq_id, batch.resource_rq_id))
                        {
                            for (resource_id, gap_amount) in gap_resources.iter_all_pairs() {
                                if gap_amount >= sn_assignment.free_resources.get(resource_id) {
                                    // The worker's own resource limit is at least as strict.
                                    continue;
                                }
                                // Gap tasks are charged at the largest variant amount, so a mix
                                // of variants can never use more of the gap than counted.
                                let largest = placement_vars
                                    .iter()
                                    .map(|(v_id, _)| {
                                        batch_rqv
                                            .get(*v_id)
                                            .get_amount(resource_id)
                                            .unwrap_or_else(|| w.resources.get(resource_id))
                                    })
                                    .max();
                                if let Some(largest) = largest
                                    && !largest.is_zero()
                                {
                                    shared_gap
                                        .entry((w.id, *blocker_rq_id, resource_id))
                                        .or_insert_with(|| (gap_amount, Vec::new()))
                                        .1
                                        .push((gap_var, largest.as_f64()));
                                }
                            }
                        }
                    }
                }
                // Workers held for the blocker take only what the priority rule allows anyway
                // (`cut.size`), with no blocker-count escape, so their capacity accumulates.
                if !cond_terms_reserved.is_empty() {
                    solver.set_name(|| {
                        format!(
                            "limit #rq{} to {} on workers held for rq{blocker_rq_id}",
                            batch.resource_rq_id, cut.size
                        )
                    });
                    solver.add_constraint(
                        ConstraintType::Max,
                        cut.size as f64,
                        cond_terms_reserved.iter().copied(),
                    );
                }
                if cond_terms.is_empty() {
                    continue;
                }
                // The before-count is one number for the whole cluster: every placement counts
                // against it, except what sits in the gap of its worker.
                if let Some(s) = blocking_size
                    && let Some(blocking_v) = get_bvar(&mut solver, *blocker_rq_id, *s)
                {
                    solver.set_name(|| {
                        format!(
                            "if #rq{blocker_rq_id} < {s} then limit #rq{} to {} where both rqs may run",
                            batch.resource_rq_id, cut.size
                        )
                    });
                    let cut_size = cut.size as f64;
                    solver.add_constraint(
                        ConstraintType::Max,
                        batch_size + cut_size,
                        cond_terms
                            .iter()
                            .copied()
                            .chain(std::iter::once((blocking_v, batch_size))),
                    );
                } else if blocking_size.is_none()
                    && (cond_terms.iter().any(|(_, coef)| *coef < 0.0)
                        || !blocked_by_unbounded.contains(blocker_rq_id))
                {
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
                        cond_terms.iter().copied(),
                    );
                }
            }
        }
    }

    // One gap per worker and blocker, shared by every request the blocker blocks.
    for ((worker_id, blocker_rq_id, resource_id), (gap_amount, terms)) in shared_gap {
        solver.set_name(|| {
            format!(
                "w{worker_id}: gap of rq{blocker_rq_id} on resource {} is {gap_amount}",
                resource_id.as_num()
            )
        });
        solver.add_constraint(ConstraintType::Max, gap_amount.as_f64(), terms.into_iter());
    }

    let mut result = SchedulingSolution::default();
    let Some((solution, is_optimal)) = solver.solve_bounded(scheduler_state.config.mip_time_limit)
    else {
        return result;
    };
    result.is_optimal = is_optimal;

    for batch in task_batches {
        let resource_rq_id = batch.resource_rq_id;
        let rqv = request_map.get(resource_rq_id);
        if rqv.is_multi_node() {
            let v_id = ResourceVariantId::new(0);
            let n_nodes = rqv.get(v_id).n_nodes() as usize;
            let mut ws: Vec<ThinVec<WorkerId>> = Vec::new();
            // Chunks are cut per worker group: the group constraint makes each group's count a
            // multiple of `n_nodes`, but worker ids of different groups may interleave, so cutting
            // across all workers in id order could give a task nodes from two groups.
            let mut open: Map<&str, ThinVec<WorkerId>> = Map::new();
            for worker in &workers {
                if let Some(v) = placements.get(&(worker.id, resource_rq_id, v_id)) {
                    let count = solution.get_value(*v).round() as u32;
                    if count > 0 {
                        let group = worker.configuration.group.as_str();
                        let chunk = open
                            .entry(group)
                            .or_insert_with(|| ThinVec::with_capacity(n_nodes));
                        chunk.push(worker.id);
                        if chunk.len() == n_nodes {
                            ws.push(open.remove(group).unwrap());
                        }
                    }
                }
            }
            assert!(open.is_empty());
            if !ws.is_empty() {
                result.mn_workers.insert((resource_rq_id, v_id), ws);
            }
        } else {
            for v_id in rqv.variant_ids() {
                let counts: Map<_, _> = workers
                    .iter()
                    .filter_map(|w| {
                        placements.get(&(w.id, resource_rq_id, v_id)).and_then(|v| {
                            let count = solution.get_value(*v).round() as u32;
                            if count > 0 { Some((w.id, count)) } else { None }
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
    let all_cpus_amount = worker.resources.get(CPU_RESOURCE_ID);
    if all_cpus_amount.is_max() {
        // Partial worker with unknown CPU capacity
        // so they always satisfy any min_utilization threshold; no LP constraint needed.
        return;
    }
    let all_cpus = all_cpus_amount.as_f64();
    let free_cpus = sn.free_resources.get(CPU_RESOURCE_ID).as_f64();
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

/// Whether a single-node variant of a request can be placed on the worker in this round.
/// Shared by placement creation and by `held_workers`, which must agree on it exactly.
fn sn_variant_fits_now(
    worker: &Worker,
    rq_id: ResourceRqId,
    v_idx: ResourceVariantId,
    rq: &ResourceRequest,
    now: std::time::Instant,
) -> bool {
    !worker.is_request_blocked(rq_id, v_idx)
        && worker.has_time_to_run(rq.min_time(), now)
        && worker.have_immediate_resources_for_rq(rq)
}

/// The gap allowance of `worker` for blocker `rq_id`, given what currently runs there.
fn worker_gap(
    worker: &Worker,
    rq_id: ResourceRqId,
    request_map: &ResourceRqMap,
    task_map: &TaskMap,
    scheduler_state: &SchedulerState,
) -> Option<WorkerResources> {
    let a = worker.sn_assignment()?;
    scheduler_state.gap_cache.gap_resources(
        rq_id,
        &worker.resources,
        a.assigned_tasks.iter().map(|task_id| {
            let t = task_map.get_task(*task_id);
            (
                t.resource_rq_id,
                t.assigned_placement(&scheduler_state.redirects).unwrap().1,
            )
        }),
        request_map,
    )
}

/// Workers held for one single-node blocker, most empty first (ties to the higher worker id).
struct Held {
    /// Blocker tasks that the workers' currently free resources can host.
    placeable: u32,
    /// Held workers for the blocker's largest threshold; any smaller threshold holds a prefix.
    order: Vec<WorkerId>,
}

impl Held {
    /// Whether a priority condition that asks for `threshold` blocker tasks holds this worker:
    /// it needs one held worker per blocker task it counts that cannot be placed now.
    fn holds(&self, worker_id: WorkerId, threshold: u32) -> bool {
        let needed = threshold.saturating_sub(self.placeable) as usize;
        self.order.iter().take(needed).any(|w| *w == worker_id)
    }
}

/// Workers held for each single-node blocker.
///
/// Holding is sized by the thresholds of the priority conditions that name the blocker, not by
/// its batch: a condition asking for `s` blocker tasks needs `s - placeable` held workers, and
/// blocker tasks below every request they could block need none. Sizing by the whole batch
/// would hold capacity for blocker tasks that the blocked request outranks.
fn held_workers(
    workers: &[&Worker],
    task_batches: &[TaskBatch],
    request_map: &ResourceRqMap,
    task_map: &TaskMap,
    scheduler_state: &SchedulerState,
    now: std::time::Instant,
) -> Map<ResourceRqId, Held> {
    let mut max_threshold: Map<ResourceRqId, u32> = Map::new();
    for batch in task_batches {
        for cut in &batch.cuts {
            for (blocker_rq_id, blocking_size) in &cut.blockers {
                if let Some(size) = blocking_size {
                    let entry = max_threshold.entry(*blocker_rq_id).or_default();
                    *entry = (*entry).max(*size);
                }
            }
        }
    }

    let mut held = Map::new();
    for batch in task_batches {
        let rqv = request_map.get(batch.resource_rq_id);
        if !batch.is_blocker || rqv.is_multi_node() {
            continue;
        }
        let Some(threshold) = max_threshold.get(&batch.resource_rq_id).copied() else {
            continue;
        };
        let mut placeable = 0u32;
        let mut candidates: Vec<(f64, WorkerId)> = Vec::new();
        for worker in workers {
            let Some(a) = worker.sn_assignment() else {
                continue;
            };
            let fits_now = rqv.requests_with_ids().any(|(v_idx, rq)| {
                sn_variant_fits_now(worker, batch.resource_rq_id, v_idx, rq, now)
            });
            if fits_now {
                placeable = placeable.saturating_add(a.free_resources.task_max_count(rqv));
            } else if worker.is_capable_to_run_rqv(rqv, now) {
                // The blocker cannot run here now, but this worker could host it once it drains.
                // Rank by the free resources the blocker can use: its gap is lost to it anyway.
                // Tasks placed into the gap lower free resources and gap equally, so they never
                // move the hold, and a finishing task never lowers the rank. A held worker
                // therefore loses its hold only to a worker that offers the blocker more.
                let mut usable = a.free_resources.clone();
                if let Some(gap) = worker_gap(
                    worker,
                    batch.resource_rq_id,
                    request_map,
                    task_map,
                    scheduler_state,
                ) {
                    for (res_id, amount) in gap.iter_nonzero_pairs() {
                        usable.set(res_id, usable.get(res_id).saturating_sub(amount));
                    }
                }
                candidates.push((fit_ratio(&usable, rqv), worker.id));
            }
        }
        let needed = threshold.saturating_sub(placeable) as usize;
        if needed == 0 {
            continue;
        }
        candidates.sort_unstable_by(|(fit_a, w_a), (fit_b, w_b)| {
            fit_b.total_cmp(fit_a).then(w_b.cmp(w_a))
        });
        held.insert(
            batch.resource_rq_id,
            Held {
                placeable,
                order: candidates
                    .into_iter()
                    .take(needed)
                    .map(|(_fit, w_id)| w_id)
                    .collect(),
            },
        );
    }
    held
}

/// A lower bound on the objective weight of any single-node placement this round.
///
/// `create_sn_var` weighs a placement by its request's share of the free resources, the worker's
/// compaction bias `(n - i_w) / n`, and the request weight. The bias is smallest on the last
/// worker, `1 / n`, and a request for *all* of a resource is valued at the worker's total, which is
/// at least the smallest total any worker has. Only positive weights count; if there are none,
/// nothing can be unlocked and the scale is irrelevant.
fn placement_weight_lower_bound(
    workers: &[&Worker],
    task_batches: &[TaskBatch],
    request_map: &ResourceRqMap,
    resource_sums: &[f64],
) -> f64 {
    let n_workers = workers.len();
    if n_workers == 0 {
        return 1.0;
    }
    let smallest_total = |r: ResourceId| {
        workers
            .iter()
            .map(|w| w.resources.get(r).as_f64())
            .filter(|amount| *amount > 0.0)
            .fold(f64::INFINITY, f64::min)
    };
    let mut bound = f64::INFINITY;
    for batch in task_batches {
        for rq in request_map.get(batch.resource_rq_id).requests() {
            if rq.is_multi_node() {
                continue;
            }
            let share: f64 = rq
                .entries()
                .iter()
                .map(|e| {
                    let global = resource_sums[e.resource_id.as_usize()];
                    if global < 0.000001 {
                        return 0.0;
                    }
                    let amount = e
                        .request
                        .amount_or_none_if_all()
                        .map(|a| a.as_f64())
                        .unwrap_or_else(|| smallest_total(e.resource_id));
                    if amount.is_finite() {
                        amount / global
                    } else {
                        0.0
                    }
                })
                .sum();
            let weight = share * rq.weight().as_f64() / n_workers as f64;
            if weight > 0.0 {
                bound = bound.min(weight);
            }
        }
    }
    if bound.is_finite() { bound } else { 1.0 }
}

fn fit_ratio(free: &WorkerResources, rqv: &ResourceRequestVariants) -> f64 {
    rqv.requests()
        .iter()
        .map(|rq| {
            rq.entries()
                .iter()
                .map(|e| {
                    let available = free.get(e.resource_id).total_fractions() as f64;
                    match e.request.amount_or_none_if_all() {
                        // `all` is only satisfied by an untouched resource, so anything less is 0.
                        None => 0.0,
                        Some(required) if required.is_zero() => 1.0,
                        Some(required) => (available / required.total_fractions() as f64).min(1.0),
                    }
                })
                .fold(f64::INFINITY, f64::min)
        })
        .fold(0.0, f64::max)
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
        .iter_nonzero_pairs()
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

/// How many tasks of `rqv` fit into a worker's gap allowance for some blocker.
/// Worker, blocker request and resource: one shared gap allowance.
type SharedGapKey = (WorkerId, ResourceRqId, ResourceId);

fn gap_count(gap_resources: &WorkerResources, rqv: &ResourceRequestVariants) -> u32 {
    if rqv.is_multi_node() {
        return 0;
    }
    rqv.requests()
        .iter()
        .map(|rq| gap_resources.task_max_count_for_request(rq))
        .min()
        .unwrap_or(0)
}

fn constraint_extra_var(
    solver: &mut LpSolver,
    constraint_type: ConstraintType,
    limit_value: f64,
    vars: impl Iterator<Item = Variable>,
    var: Variable,
    coef: f64,
) {
    solver.add_constraint(
        constraint_type,
        limit_value,
        vars.map(|v| (v, 1.0)).chain(std::iter::once((var, coef))),
    );
}
