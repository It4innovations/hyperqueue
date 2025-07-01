use crate::control::{NewWorkerAllocationResponse, WorkerTypeQuery};
use crate::gateway::MultiNodeAllocationResponse;
use crate::internal::server::core::Core;
use crate::internal::server::task::Task;
use crate::internal::server::workerload::{WorkerLoad, WorkerResources};
use std::time::Duration;

struct WorkerTypeState {
    partial: bool,
    loads: Vec<WorkerLoad>,
    w_resources: WorkerResources,
    time_limit: Option<Duration>,
    max: u32,
    min: u32,
}

/// Read the documentation of `new_worker_query`` in control.rs
pub(crate) fn compute_new_worker_query(
    core: &mut Core,
    queries: &[WorkerTypeQuery],
) -> NewWorkerAllocationResponse {
    log::debug!("Compute new worker query: query = {queries:?}");

    // Scheduler has to be performed before the query, so there should be no ready_to_assign tasks
    assert!(core.sn_ready_to_assign().is_empty() || !core.has_workers());

    let add_task = |new_loads: &mut [WorkerTypeState], task: &Task| {
        let request = &task.configuration.resources;
        for ws in new_loads.iter_mut() {
            if !ws.w_resources.is_capable_to_run_with(request, |rq| {
                ws.time_limit.is_none_or(|t| rq.min_time() <= t)
            }) {
                if ws.partial
                    && ws.w_resources.is_lowerbound_for(request, |rq| {
                        ws.time_limit.is_none_or(|t| rq.min_time() <= t)
                    })
                {
                    ws.min = 1;
                }
                continue;
            }
            for load in ws.loads.iter_mut() {
                if load.have_immediate_resources_for_rqv(request, &ws.w_resources) {
                    load.add_request(task.id, request, &ws.w_resources);
                    return;
                }
            }
            if ws.loads.len() < ws.max as usize {
                let mut load = WorkerLoad::new(&ws.w_resources);
                load.add_request(task.id, request, &ws.w_resources);
                ws.loads.push(load);
                return;
            }
        }
    };

    /* Make sure that all named resources provided has an Id */
    for query in queries {
        for resource in &query.descriptor.resources {
            core.get_or_create_resource_id(&resource.name);
        }
    }

    let resource_map = core.create_resource_map();
    let mut new_loads: Vec<_> = queries
        .iter()
        .map(|q| WorkerTypeState {
            partial: q.partial,
            loads: Vec::new(),
            w_resources: WorkerResources::from_description(&q.descriptor, &resource_map),
            time_limit: q.time_limit,
            max: q.max_sn_workers,
            min: 0,
        })
        .collect();

    for worker in core.get_workers() {
        let mut load = WorkerLoad::new(&worker.resources);
        for task_id in worker.sn_tasks() {
            let task = core.get_task(*task_id);
            let request = &task.configuration.resources;
            if task.is_sn_running()
                || load.have_immediate_resources_for_rqv(request, &worker.resources)
            {
                load.add_request(task.id, request, &worker.resources);
                continue;
            }
            add_task(&mut new_loads, task);
        }
    }
    for task_id in core.sleeping_sn_tasks() {
        let task = core.get_task(*task_id);
        add_task(&mut new_loads, task);
    }

    // `compute_new_worker_query` should be called immediately after scheduling was performed,
    // so read_to_assign should be usually already processed.
    // However, scheduler is lazy and if there is no worker at all it will do nothing, even
    // postponing ready_to_assign. So we have to look also into this array
    for task_id in core.sn_ready_to_assign() {
        let task = core.get_task(*task_id);
        add_task(&mut new_loads, task);
    }

    let single_node_allocations: Vec<u32> = new_loads
        .iter()
        .zip(queries.iter())
        .map(|(ws, q)| {
            ws.loads
                .iter()
                .map(|load| {
                    if load.utilization(&ws.w_resources) >= q.min_utilization {
                        1
                    } else {
                        0
                    }
                })
                .sum::<u32>()
                .max(ws.min)
        })
        .collect();

    let (queue, _map, _ws) = core.multi_node_queue_split();
    let mut multi_node_allocations: Vec<_> = queue
        .get_profiles()
        .filter_map(|(rq, count)| {
            let n_nodes = rq.n_nodes();
            queries.iter().enumerate().find_map(|(i, worker_type)| {
                if let Some(time_limit) = worker_type.time_limit {
                    if rq.min_time() > time_limit {
                        return None;
                    }
                }
                if worker_type.max_workers_per_allocation >= n_nodes {
                    Some(MultiNodeAllocationResponse {
                        worker_type: i,
                        worker_per_allocation: n_nodes,
                        max_allocations: count,
                    })
                } else {
                    None
                }
            })
        })
        .collect();
    multi_node_allocations.sort_unstable_by_key(|x| (x.worker_type, x.worker_per_allocation));

    NewWorkerAllocationResponse {
        single_node_workers_per_query: single_node_allocations,
        multi_node_allocations,
    }
}
