use crate::internal::common::resources::ResourceId;
use crate::internal::scheduler2::TaskBatch;
use crate::internal::server::core::Core;
use crate::internal::solver::{LpInnerSolver, LpSolution, LpSolver, Variable};
use crate::resources::ResourceRqId;
use crate::{Map, ResourceVariantId, TaskId, WorkerId};
use hashbrown::Equivalent;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(crate) struct WorkerTaskMapping {
    pub(crate) task_to_workers: Map<WorkerId, Vec<(TaskId, ResourceVariantId)>>,
}

pub(crate) fn run_scheduling_solver(
    core: &mut Core,
    now: std::time::Instant,
    task_batches: &[TaskBatch],
) -> WorkerTaskMapping {
    let n_resources = core.resource_map_mut().n_resources();

    let (task_map, worker_map, task_queues, resource_map, _) = core.split_all();
    let Some(n_variants) = resource_map.iter().map(|r| r.requests().len()).max() else {
        return WorkerTaskMapping::default();
    };
    let mut global_resource_sums = vec![0f64; n_resources];

    let mut workers = worker_map.get_workers().collect::<Vec<_>>();
    workers.sort_unstable_by_key(|w| w.id);
    workers.iter().for_each(|worker| {
        global_resource_sums
            .iter_mut()
            .zip(worker.free_resources.iter())
            .for_each(|(s, c)| *s += c.as_f64())
    });

    let n_workers = worker_map.len();
    let n_batches = task_batches.len();

    let mut solver = LpSolver::new(true);

    let mut placements: Map<(WorkerId, ResourceRqId, ResourceVariantId), (_, u32)> = Map::new();
    let mut batch_vars: Map<ResourceRqId, Vec<_>> = Map::new();

    // let mut placement_vars: Vec<Option<_>> = vec![None; n_workers * n_batches * n_variants];
    //
    // let placement_idx = |worker_idx: usize, batch_idx: usize, variant: usize| {
    //     worker_idx * n_batches * n_variants + batch_idx * n_variants + variant
    // };

    let mut worker_res_constraint = vec![Vec::new(); n_resources];
    // Create worker-task placements
    for (w_idx, worker) in workers.iter().enumerate() {
        for (b_idx, batch) in task_batches.iter().enumerate() {
            let rqv = resource_map.get(batch.resource_rq_id);
            for (v_idx, rq) in rqv.requests_with_ids() {
                assert!(!rq.is_multi_node());
                if worker.has_time_to_run(rq.min_time(), now)
                    && worker.have_immediate_resources_for_rq(rq)
                {
                    let weight = rq
                        .entries()
                        .iter()
                        .map(|e| {
                            let r = e.resource_id;
                            let global = global_resource_sums[r.as_usize()];
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
                        / n_workers as f64;
                    solver.name_var(|| {
                        let mut s = format!("p{}:{}", worker.id, batch.resource_rq_id);
                        if v_idx.is_first() {
                            use std::fmt::Write;
                            write!(&mut s, ":{}", v_idx).unwrap();
                        }
                        s
                    });
                    let v = solver.add_nat_variable(weight);
                    let new_idx = placements.len() as u32;
                    placements.insert((worker.id, batch.resource_rq_id, v_idx), (v, new_idx));
                    batch_vars.entry(batch.resource_rq_id).or_default().push(v);

                    // Insert into worker resource constraints
                    for e in rq.entries() {
                        let r = e.resource_id;
                        worker_res_constraint[r.as_usize()].push((
                            v,
                            e.request
                                .amount_or_none_if_all()
                                .unwrap_or_else(|| worker.resources.get(r))
                                .as_f64(),
                        ));
                    }
                }
            }
        }
        // Create worker constraints
        for (r, c) in worker_res_constraint.iter_mut().enumerate() {
            if !c.is_empty() {
                solver.add_max_constraint(
                    worker
                        .free_resources
                        .get(ResourceId::new(r as u32))
                        .as_f64(),
                    c.iter().copied(),
                )
            }
            c.clear();
        }
    }

    // blocking_variable_vars[(rq_id, s)] is True only if there is at least
    // `s` tasks of `rq_id` scheduled
    let mut blocked_priority_vars: Map<(ResourceRqId, u32), _> = Map::new();
    //let mut tmp = Vec::new();

    for batch in task_batches.iter() {
        let rqv = resource_map.get(batch.resource_rq_id);
        let vars = batch_vars.get(&batch.resource_rq_id).unwrap();
        if !batch.limit_reached && !vars.is_empty() {
            solver.add_max_constraint(batch.size as f64, vars.iter().map(|v| (*v, 1.0)))
        }
        let batch_size = batch.size as f64;
        for cut in &batch.cuts {
            //tmp.clear();
            //dbg!(cut);
            for (rq_id, size) in &cut.blockers {
                if let Some(s) = size {
                    let blocking_v =
                        *blocked_priority_vars
                            .entry((*rq_id, *s))
                            .or_insert_with(|| {
                                // Create a new blocking variable
                                solver.name_var(|| format!("B{}~{}", rq_id, s));
                                let new_v = solver.add_bool_variable(0.0);
                                let vars = batch_vars.get(rq_id).unwrap();
                                let bound = *s as f64;
                                min_extra_var(&mut solver, bound, &vars, new_v, -bound);
                                new_v
                            });
                    max_extra_var(&mut solver, batch_size, &vars, blocking_v, batch_size);
                } else {
                    todo!()
                }
            }
        }
    }

    let mut result = WorkerTaskMapping::default();
    let Some((solution, _)) = solver.solve() else {
        return result;
    };

    let values = solution.get_values();

    for batch in task_batches {
        let resource_rq_id = batch.resource_rq_id;
        let rqv = resource_map.get(batch.resource_rq_id);
        for v_id in rqv.variant_ids() {
            let mut counts: Vec<_> = workers
                .iter()
                .filter_map(|w| {
                    placements
                        .get(&(w.id, batch.resource_rq_id, v_id))
                        .map(|(_, var_idx)| (w.id, values[*var_idx as usize].round() as u32))
                })
                .collect();
            let sum = counts.iter().map(|(_, c)| c).sum::<u32>();
            if sum == 0 {
                continue;
            }
            let tasks = task_queues[resource_rq_id.as_usize()].take_tasks(sum);
            let mut task_idx = 0;
            'outer: loop {
                for (w_id, c) in &mut counts {
                    if *c > 0 {
                        *c -= 1;
                        result
                            .task_to_workers
                            .entry(*w_id)
                            .or_default()
                            .push((tasks[task_idx], v_id));
                        task_idx += 1;
                        if task_idx >= tasks.len() {
                            break 'outer;
                        }
                    }
                }
            }
        }
        //workers.iter().map(|w| )
    }

    /*for ((worker_id, batch_idx, v_idx), size) in placement_vars.iter().zip(solution.get_values()) {
        let size = size.round() as usize;
        if size > 0 {
            let task_to_workers = if let Some(tw) = result
                .task_to_workers
                .last_mut()
                .filter(|w| w.0 == *worker_id)
            {
                tw
            } else {
                result.task_to_workers.push((*worker_id, Vec::new()));
                result.task_to_workers.last_mut().unwrap()
            };
        }
    }*/

    result
}

fn min_extra_var(
    solver: &mut LpSolver,
    min_value: f64,
    vars: &[Variable],
    var: Variable,
    coef: f64,
) {
    solver.add_min_constraint(
        min_value,
        vars.iter()
            .map(|v| (*v, 1.0))
            .chain(std::iter::once((var, coef))),
    );
}

fn max_extra_var(
    solver: &mut LpSolver,
    max_value: f64,
    vars: &[Variable],
    var: Variable,
    coef: f64,
) {
    solver.add_max_constraint(
        max_value,
        vars.iter()
            .map(|v| (*v, 1.0))
            .chain(std::iter::once((var, coef))),
    );
}
