use crate::control::{NewWorkerAllocationResponse, WorkerTypeQuery};
use crate::gateway::MultiNodeAllocationResponse;
use crate::internal::server::core::Core;
use crate::internal::server::workerload::{WorkerLoad, WorkerResources};

/* Read the documentation of NewWorkerQuery in gateway.rs */
pub(crate) fn compute_new_worker_query(
    core: &Core,
    queries: &[WorkerTypeQuery],
) -> NewWorkerAllocationResponse {
    assert!(core.sn_ready_to_assign().is_empty()); // If there are read_to_assign tasks, first we have to call scheduling
    log::debug!("Compute new worker query: query = {:?}", queries);

    let mut free_tasks = Vec::new();
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
            free_tasks.push(*task_id);
        }
    }
    log::debug!(
        "Compute new worker query: free_tasks = {}",
        free_tasks.len()
    );
    free_tasks.extend(core.sleeping_sn_tasks());
    let resource_map = core.create_resource_map();
    let mut new_loads: Vec<_> = queries
        .iter()
        .map(|q| {
            (
                Vec::<WorkerLoad>::new(),
                WorkerResources::from_description(&q.descriptor, &resource_map),
                q.max_sn_workers,
            )
        })
        .collect();
    'outer: for task_id in free_tasks {
        let task = core.get_task(task_id);
        let request = &task.configuration.resources;
        for (loads, wr, max_workers) in new_loads.iter_mut() {
            if !wr.is_capable_to_run(request) {
                continue;
            }
            for load in loads.iter_mut() {
                if load.have_immediate_resources_for_rqv(request, wr) {
                    load.add_request(task_id, request, wr);
                    continue 'outer;
                }
            }
            if loads.len() < (*max_workers) as usize {
                let mut load = WorkerLoad::new(wr);
                load.add_request(task_id, request, wr);
                loads.push(load);
                continue 'outer;
            }
        }
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
        single_node_allocations: new_loads.iter().map(|x| x.0.len()).collect(),
        multi_node_allocations,
    }
}
