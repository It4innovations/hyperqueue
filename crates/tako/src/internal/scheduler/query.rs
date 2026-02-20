use crate::control::{NewWorkerAllocationResponse, WorkerTypeQuery};
use crate::gateway::MultiNodeAllocationResponse;
use crate::internal::common::resources::ResourceId;
use crate::internal::scheduler::{create_task_batches, create_task_mapping, run_scheduling_solver};
use crate::internal::server::core::{Core, CoreSplit};
use crate::internal::server::task::Task;
use crate::internal::server::worker::Worker;
use crate::internal::server::workerload::{WorkerLoad, WorkerResources};
use crate::resources::{ResourceAmount, ResourceDescriptorItem, ResourceDescriptorKind};
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use crate::{Map, WorkerId};
use std::time::Duration;

/// Read the documentation of `new_worker_query`` in control.rs
pub(crate) fn compute_new_worker_query(
    core: &mut Core,
    queries: &[WorkerTypeQuery],
) -> NewWorkerAllocationResponse {
    log::debug!("Compute new worker query: query = {queries:?}");

    let fake_worker_id_base = core.worker_counter() + 1;
    let mut fake_worker_counter = fake_worker_id_base;
    let resource_map = core.resource_map().create_resource_id_map();
    let mut fake_workers = Vec::new();
    let now = std::time::Instant::now();
    queries.iter().for_each(|query| {
        for _ in 0..query.max_sn_workers {
            let mut resources = query.descriptor.clone();

            if query.partial {
                // If query is partial, add a fake maximal resources of unknown size
                for name in resource_map.iter_names() {
                    if resources
                        .resources
                        .iter()
                        .find(|r| r.name == *name)
                        .is_none()
                    {
                        resources.resources.push(ResourceDescriptorItem {
                            name: name.to_string(),
                            kind: ResourceDescriptorKind::Sum {
                                size: ResourceAmount::MAX,
                            },
                        })
                    }
                }
            }
            let worker_id = WorkerId::new(fake_worker_counter);
            fake_worker_counter += 1;
            let configuration = WorkerConfiguration {
                resources,
                time_limit: query.time_limit,
                listen_address: String::new(),
                hostname: String::new(),
                group: format!("fake-worker-group-{worker_id}"),
                work_dir: Default::default(),
                heartbeat_interval: Default::default(),
                overview_configuration: Default::default(),
                idle_timeout: None,
                on_server_lost: ServerLostPolicy::Stop,
                max_parallel_downloads: 0,
                max_download_tries: 0,
                wait_between_download_tries: Default::default(),
                extra: Default::default(),
            };
            let worker = Worker::new(worker_id, configuration, &resource_map, now);
            fake_workers.push(worker);
        }
    });

    let batches = create_task_batches(core, now, &fake_workers);
    let scheduling = run_scheduling_solver(core, now, &batches, &fake_workers);

    let mut loads: Map<WorkerId, WorkerResources> = Map::new();

    let CoreSplit {
        request_map,
        task_queues,
        ..
    } = core.split();
    for ((resource_rq_id, rv_id), workers) in scheduling.sn_counts {
        let rq = request_map.get(resource_rq_id).get(rv_id);
        for (worker_id, count) in workers {
            if worker_id.as_num() < fake_worker_id_base {
                continue;
            }
            let idx = worker_id.as_num() - fake_worker_id_base;
            let load = loads
                .entry(worker_id)
                .or_insert_with(|| WorkerResources::empty(resource_map.size()));
            load.add_multiple(rq, &fake_workers[idx as usize].resources, count);
        }
    }

    let mut single_node_workers_per_query = Vec::with_capacity(queries.len());
    let mut worker_idx = 0;
    for query in queries {
        let mut count = 0;
        for _ in 0..query.max_sn_workers {
            let worker = &fake_workers[worker_idx];
            worker_idx += 1;
            if let Some(load) = loads.get(&worker.id) {
                if load.utilization(&worker.resources) >= query.min_utilization {
                    count += 1;
                }
            }
        }
        single_node_workers_per_query.push(count);
    }

    let mut multi_node_allocations: Vec<_> = task_queues
        .iter()
        .filter_map(|queue| {
            let rqv = core.get_resource_rq(queue.resource_rq_id);
            if !rqv.is_multi_node() {
                return None;
            }
            let rq = rqv.unwrap_first();
            let n_nodes = rq.n_nodes();
            queries.iter().enumerate().find_map(|(i, worker_type)| {
                if let Some(time_limit) = worker_type.time_limit
                    && rq.min_time() > time_limit
                {
                    return None;
                }
                if worker_type.max_workers_per_allocation >= n_nodes {
                    Some(MultiNodeAllocationResponse {
                        worker_type: i,
                        worker_per_allocation: n_nodes,
                        max_allocations: queue.size(),
                    })
                } else {
                    None
                }
            })
        })
        .collect();
    multi_node_allocations.sort_unstable_by_key(|x| (x.worker_type, x.worker_per_allocation));

    NewWorkerAllocationResponse {
        single_node_workers_per_query,
        multi_node_allocations,
    }
}
