use crate::control::{NewWorkerAllocationResponse, WorkerTypeQuery};
use crate::gateway::MultiNodeAllocationResponse;
use crate::internal::scheduler::{create_task_batches, run_scheduling_solver};
use crate::internal::server::core::{Core, CoreSplit};
use crate::internal::server::worker::Worker;
use crate::resources::{ResourceAmount, ResourceDescriptorItem, ResourceDescriptorKind};
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use crate::{Set, WorkerId};
use std::time::Duration;

/// What the query's MILP cost, as opposed to its answer. A query runs the whole scheduling
/// pipeline over real + fake workers, so it has a model size and can hit `mip_time_limit` like
/// any other round -- and because it runs under an exclusive borrow of `Core`, that cost is
/// server-thread blocking time. Measured by the E8 sweep in `sim.rs`; nothing in production
/// reads it, hence the `dead_code` allowance off the `sim` feature.
#[cfg_attr(not(feature = "sim"), allow(dead_code))]
pub(crate) struct QuerySolveStats {
    pub n_variables: u32,
    pub n_constraints: u32,
    /// `false` if the solve hit `mip_time_limit`, i.e. the answer is truncated rather than cheap.
    pub is_optimal: bool,
    /// Fake workers instantiated, summed over the query types.
    pub n_fake_workers: usize,
}

/// Read the documentation of `new_worker_query`` in control.rs
pub(crate) fn compute_new_worker_query(
    core: &mut Core,
    queries: &[WorkerTypeQuery],
) -> NewWorkerAllocationResponse {
    compute_new_worker_query_with_stats(core, queries).0
}

/// `compute_new_worker_query`, additionally reporting what the solve cost. Kept as the single
/// implementation so the evaluation measures the production path rather than a copy of it.
pub(crate) fn compute_new_worker_query_with_stats(
    core: &mut Core,
    queries: &[WorkerTypeQuery],
) -> (NewWorkerAllocationResponse, QuerySolveStats) {
    log::debug!("Compute new worker query: query = {queries:?}");

    let fake_worker_id_base = core.worker_counter() + 1;
    let mut fake_worker_counter = fake_worker_id_base;

    /* Make sure that all resources provided by Worker has an Id */
    for query in queries {
        for item in &query.descriptor.resources {
            core.get_or_create_resource_id(&item.name);
        }
    }

    let resource_map = core.resource_map().create_resource_id_map();
    let mut fake_workers = Vec::new();
    let now = std::time::Instant::now();

    queries.iter().for_each(|query| {
        for _ in 0..query.max_sn_workers {
            let mut resources = query.descriptor.clone();
            if query.partial {
                // If query is partial, add a fake maximal resources for resource that was not explicitly defined
                for name in resource_map.iter_names() {
                    if !resources.resources.iter().any(|r| r.name == *name) {
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
                min_utilization: query.min_utilization,
                extra: Default::default(),
                retract_check_interval: Duration::from_secs(30),
            };
            let worker = Worker::new(worker_id, configuration, &resource_map, now);
            fake_workers.push(worker);
        }
    });

    let batches = create_task_batches(core, now, Some(fake_workers.as_slice()));
    let scheduling = run_scheduling_solver(core, now, &batches, Some(fake_workers.as_slice()));
    let solve_stats = QuerySolveStats {
        n_variables: scheduling.n_variables,
        n_constraints: scheduling.n_constraints,
        is_optimal: scheduling.is_optimal,
        n_fake_workers: fake_workers.len(),
    };

    let mut is_loaded: Set<WorkerId> = Set::new();

    for workers in scheduling.sn_counts.values() {
        for (worker_id, count) in workers {
            if *count > 0 {
                is_loaded.insert(*worker_id);
            }
        }
    }

    let mut single_node_workers_per_query = Vec::with_capacity(queries.len());
    let mut worker_idx = 0;
    for query in queries {
        let mut count = 0;
        for _ in 0..query.max_sn_workers {
            let worker = &fake_workers[worker_idx];
            worker_idx += 1;
            if is_loaded.contains(&worker.id) {
                count += 1;
            }
        }
        single_node_workers_per_query.push(count);
    }

    let CoreSplit { task_queues, .. } = core.split();
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

    (
        NewWorkerAllocationResponse {
            single_node_workers_per_query,
            multi_node_allocations,
        },
        solve_stats,
    )
}
