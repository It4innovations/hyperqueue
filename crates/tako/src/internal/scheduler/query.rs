use crate::control::{NewWorkerAllocationResponse, WorkerTypeQuery};
use crate::gateway::MultiNodeAllocationResponse;
use crate::internal::server::core::Core;
use crate::internal::server::task::Task;
use crate::internal::server::workerload::{WorkerLoad, WorkerResources};

struct WorkerTypeState {
    loads: Vec<WorkerLoad>,
    w_resources: WorkerResources,
    max: u32,
}

/* Read the documentation of NewWorkerQuery in gateway.rs */
pub(crate) fn compute_new_worker_query(
    core: &Core,
    queries: &[WorkerTypeQuery],
) -> NewWorkerAllocationResponse {
    assert!(core.sn_ready_to_assign().is_empty()); // If there are read_to_assign tasks, first we have to call scheduling
    log::debug!("Compute new worker query: query = {:?}", queries);

    let add_task = |new_loads: &mut [WorkerTypeState], task: &Task| {
        let request = &task.configuration.resources;
        for ws in new_loads.iter_mut() {
            if !ws.w_resources.is_capable_to_run(request) {
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
    let resource_map = core.create_resource_map();
    let mut new_loads: Vec<_> = queries
        .iter()
        .map(|q| WorkerTypeState {
            loads: Vec::new(),
            w_resources: WorkerResources::from_description(&q.descriptor, &resource_map),
            max: q.max_sn_workers,
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

    let (queue, _map, _ws) = core.multi_node_queue_split();
    let mn_task_profiles = queue.get_profiles();

    let mut multi_node_allocations: Vec<_> = mn_task_profiles
        .iter()
        .filter_map(|(nodes, count)| {
            queries.iter().enumerate().find_map(|(i, worker_type)| {
                if worker_type.max_worker_per_allocation >= *nodes {
                    Some(MultiNodeAllocationResponse {
                        worker_type: i,
                        worker_per_allocation: *nodes,
                        max_allocations: *count,
                    })
                } else {
                    None
                }
            })
        })
        .collect();
    multi_node_allocations.sort_unstable_by_key(|x| (x.worker_type, x.worker_per_allocation));

    NewWorkerAllocationResponse {
        single_node_allocations: new_loads.iter().map(|ws| ws.loads.len()).collect(),
        multi_node_allocations,
    }
}
