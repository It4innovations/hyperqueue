use crate::common::manager::info::{ManagerInfo, ManagerType};
use crate::common::rpc::RpcReceiver;
use crate::common::utils::time::AbsoluteTime;
use crate::get_or_return;
use crate::server::autoalloc::config::{
    MAX_QUEUED_STATUS_ERROR_COUNT, MAX_RUNNING_STATUS_ERROR_COUNT, MAX_SUBMISSION_FAILS,
    SUBMISSION_DELAYS, get_allocation_refresh_interval, get_allocation_schedule_tick_interval,
    get_max_allocation_schedule_delay, max_allocation_fails,
};
use crate::server::autoalloc::queue::pbs::PbsHandler;
use crate::server::autoalloc::queue::slurm::SlurmHandler;
use crate::server::autoalloc::queue::{AllocationExternalStatus, QueueHandler, SubmitMode};
use crate::server::autoalloc::service::{AutoAllocMessage, LostWorkerDetails};
use crate::server::autoalloc::state::{
    AllocationQueue, AllocationQueueState, AllocationState, AutoAllocState, RateLimiter,
    RateLimiterStatus,
};
use crate::server::autoalloc::{
    Allocation, AllocationId, AutoAllocResult, QueueId, QueueInfo, QueueParameters,
};
use crate::server::event::streamer::EventStreamer;
use crate::transfer::messages::{QueueData, QueueState};
use anyhow::Context;
use futures::future::join_all;
use std::future::Future;
use std::path::PathBuf;
use std::time::Instant;
use tako::WorkerId;
use tako::control::{ServerRef, WorkerTypeQuery};
use tako::resources::ResourceDescriptor;
use tako::{Map, Set};
use tempfile::TempDir;

struct AutoallocSenders {
    server: ServerRef,
    events: EventStreamer,
}

/// This is the main autoalloc event loop, which periodically refreshes the autoalloc queue state
/// and also reacts to external autoalloc messages.
pub async fn autoalloc_process(
    server: ServerRef,
    events: EventStreamer,
    mut autoalloc: AutoAllocState,
    mut receiver: RpcReceiver<AutoAllocMessage>,
) {
    let senders = AutoallocSenders { server, events };

    // The loop below is a bit complicated, because it needs to manage several things:
    // - Repeatedly, we want to update the external state of allocation queues, by querying the
    // allocation manager. This is not related much to the of the functionality, and can progress
    // on its own.
    // - We are receiving messages from `receiver`, which contain information about what happened
    // in HQ (new job submits, workers connected/disconnected, etc.). Some of these messages should
    // result in new "allocation scheduling".
    // - We want to do "allocation scheduling", where we query the HQ task scheduler for what kinds
    // of workers it wants, and try to submit new allocations based on that.
    //
    // The allocation scheduling should not happen too often, it should happen after certain
    // messages are received, and it should also happen periodically if no new message was received
    // in some time. So we need to be a bit smart about when to run it.

    // Periodically update external state
    let mut periodic_update_interval = tokio::time::interval(get_allocation_refresh_interval());
    log::debug!(
        "Periodic refresh interval: {}s",
        get_allocation_refresh_interval().as_secs_f64()
    );

    // When no message is not received after this interval, run scheduling
    let max_scheduling_delay = get_max_allocation_schedule_delay();
    log::debug!(
        "Max delay between submits: {}s",
        max_scheduling_delay.as_secs_f64()
    );

    // How often to check scheduling
    let mut scheduling_interval = tokio::time::interval(get_allocation_schedule_tick_interval());
    log::debug!(
        "Submit check interval: {}s",
        get_allocation_schedule_tick_interval().as_secs_f64()
    );

    // Should scheduling be performed at the next scheduling "tick"?
    let mut should_schedule = false;
    let mut last_schedule = Instant::now();

    loop {
        tokio::select! {
            _ = periodic_update_interval.tick() => {
                if autoalloc.has_active_queues() {
                    log::debug!("Performing periodic update");
                    do_periodic_update(&senders, &mut autoalloc).await;
                }
            }
            _ = scheduling_interval.tick() => {
                if should_schedule || last_schedule.elapsed() >= max_scheduling_delay {
                    if autoalloc.has_active_queues() {
                        log::debug!("Performing submits");
                        if let Err(error) = perform_submits(&mut autoalloc, &senders).await {
                            log::error!("Autoalloc scheduling failed: {error:?}");
                        }
                    }
                    should_schedule = false;
                    last_schedule = Instant::now();
                }
            }
            msg = tokio::time::timeout(max_scheduling_delay, receiver.recv()) => {
                let schedule = match msg {
                    Ok(None) | Ok(Some(AutoAllocMessage::QuitService)) => break,
                    Ok(Some(message)) => handle_message(&mut autoalloc, &senders.events, message).await,
                    Err(_) => true
                };
                if schedule {
                    should_schedule = true;
                }
            }
        }
    }
    log::debug!("Ending autoalloc, stopping all allocations");
    stop_all_allocations(&autoalloc).await;
}

/// Try to create a temporary allocation to check that the allocation manager can work with the
/// given allocation parameters.
pub async fn try_submit_allocation(params: QueueParameters) -> anyhow::Result<()> {
    let tmpdir = TempDir::with_prefix("hq")?;
    let mut handler = create_allocation_handler(
        &params.manager,
        params.name.clone(),
        tmpdir.as_ref().to_path_buf(),
    )?;

    // Try the highest possible worker count, to ensure that it works
    let worker_count = params.max_workers_per_alloc;
    let queue_info = QueueInfo::new(params);

    let allocation = handler
        .submit_allocation(0, &queue_info, worker_count as u64, SubmitMode::DryRun)
        .await
        .map_err(|e| anyhow::anyhow!("Could not submit allocation: {:?}", e))?;

    let working_dir = allocation.working_dir().clone();
    let id = match allocation.into_id() {
        Ok(id) => id,
        Err(error) => {
            // Do not delete the directory to let the user inspect it for debugging
            let _ = tmpdir.keep();
            return Err(anyhow::anyhow!(
                "Could not submit allocation: {error:?}\nYou can find the submitted script at {}",
                working_dir.submit_script().display()
            ));
        }
    };
    let allocation = Allocation::new(id.to_string(), worker_count as u64, working_dir);
    handler
        .remove_allocation(&allocation)
        .await
        .map_err(|e| anyhow::anyhow!("Could not cancel allocation {}: {:?}", allocation.id, e))?;

    Ok(())
}

// The code doesn't compile if the Box closures are removed
#[allow(clippy::redundant_closure)]
pub fn create_allocation_handler(
    manager: &ManagerType,
    name: Option<String>,
    directory: PathBuf,
) -> anyhow::Result<Box<dyn QueueHandler>> {
    match manager {
        ManagerType::Pbs => {
            let handler = PbsHandler::new(directory, name);
            handler.map::<Box<dyn QueueHandler>, _>(|handler| Box::new(handler))
        }
        ManagerType::Slurm => {
            let handler = SlurmHandler::new(directory, name);
            handler.map::<Box<dyn QueueHandler>, _>(|handler| Box::new(handler))
        }
    }
}

/// Reacts to auto allocation message.
/// Returns `true` if scheduling should be performed after handling the message.
async fn handle_message(
    autoalloc: &mut AutoAllocState,
    events: &EventStreamer,
    message: AutoAllocMessage,
) -> bool {
    log::debug!("Handling message {message:?}");
    match message {
        AutoAllocMessage::WorkerConnected {
            id,
            config,
            manager_info,
        } => {
            log::debug!(
                "Registering worker {id} for allocation {}",
                manager_info.allocation_id
            );
            if let Some((queue, queue_id, allocation_id)) =
                get_data_from_worker(autoalloc, &manager_info)
            {
                queue.register_worker_resources(config);
                log::info!(
                    "Worker {id} connected from allocation {}",
                    manager_info.allocation_id
                );
                sync_allocation_status(
                    events,
                    queue_id,
                    queue,
                    &allocation_id,
                    AllocationSyncReason::WorkedConnected(id),
                );
            } else {
                log::warn!(
                    "Worker {id} connected from an unknown allocation {}",
                    manager_info.allocation_id
                );
            }
            true
        }
        AutoAllocMessage::WorkerLost(id, manager_info, details) => {
            log::debug!(
                "Removing worker {id} from allocation {}",
                manager_info.allocation_id
            );
            if let Some((queue, queue_id, allocation_id)) =
                get_data_from_worker(autoalloc, &manager_info)
            {
                sync_allocation_status(
                    events,
                    queue_id,
                    queue,
                    &allocation_id,
                    AllocationSyncReason::WorkerLost(id, details),
                );
            } else {
                log::warn!(
                    "Worker {id} disconnected to an unknown allocation {}",
                    manager_info.allocation_id
                );
            }
            true
        }
        AutoAllocMessage::JobSubmitted(id) => {
            log::debug!("Registering submit of job {id}");
            true
        }
        AutoAllocMessage::GetQueues(response) => {
            let queues: Map<QueueId, QueueData> = autoalloc
                .queues()
                .map(|(id, queue)| {
                    (
                        id,
                        QueueData {
                            params: queue.info().params().clone(),
                            name: queue.name().map(|name| name.to_string()),
                            manager_type: queue.manager().clone(),
                            state: match queue.state() {
                                AllocationQueueState::Active => QueueState::Active,
                                AllocationQueueState::Paused => QueueState::Paused,
                            },
                            known_worker_resources: queue.get_worker_resources().cloned(),
                        },
                    )
                })
                .collect();
            response.respond(queues);
            false
        }
        AutoAllocMessage::AddQueue {
            server_directory,
            params,
            queue_id,
            worker_resources,
            response,
        } => {
            log::debug!("Creating queue, params={params:?}");
            let result = create_queue(
                autoalloc,
                events,
                server_directory,
                params,
                queue_id,
                worker_resources,
            );
            response.respond(result);
            true
        }
        AutoAllocMessage::RemoveQueue {
            id,
            force,
            response,
        } => {
            log::debug!("Removing queue {id}");
            let result = remove_queue(autoalloc, events, id, force).await;
            response.respond(result);
            false
        }
        AutoAllocMessage::PauseQueue { id, response } => {
            let result = match autoalloc.get_queue_mut(id) {
                Some(queue) => {
                    log::debug!("Pausing queue {id}");
                    queue.pause();
                    Ok(())
                }
                None => Err(anyhow::anyhow!(
                    "Queue {id} that should have been paused was not found"
                )),
            };
            response.respond(result);
            false
        }
        AutoAllocMessage::ResumeQueue { id, response } => {
            let result = match autoalloc.get_queue_mut(id) {
                Some(queue) => {
                    log::debug!("Resuming queue {id}");
                    queue.resume();
                    Ok(())
                }
                None => Err(anyhow::anyhow!(
                    "Queue {id} that should have been resumed was not found"
                )),
            };
            response.respond(result);
            true
        }
        AutoAllocMessage::GetQueueAllocations(queue_id, response) => {
            let result = match autoalloc.get_queue(queue_id) {
                Some(queue) => Ok(queue.all_allocations().cloned().collect()),
                None => Err(anyhow::anyhow!("Queue {queue_id} not found")),
            };
            response.respond(result);
            false
        }
        AutoAllocMessage::GetAllocation(alloc_id, response) => {
            let result = match autoalloc.get_allocation_by_id(&alloc_id) {
                Some(alloc) => Ok(alloc),
                None => Err(anyhow::anyhow!("Allocation {alloc_id} not found")),
            };
            response.respond(result);
            false
        }
        AutoAllocMessage::QuitService => unreachable!(),
    }
}

/// Aggregated worker query response for a **single auto-allocation queue**.
#[derive(Debug, Copy, Clone)]
struct QueryResponse {
    single_node_workers: u32,
    multinode_allocations: u32,
    multinode_workers_per_alloc: u32,
}

impl QueryResponse {
    fn is_empty(&self) -> bool {
        self.single_node_workers == 0 && self.multinode_allocations == 0
    }
}

/// Perform an allocation submission pass.
/// Ask the scheduler how many workers it wants to be created, and spawn them into the individual
/// allocation queues.
async fn perform_submits(
    autoalloc: &mut AutoAllocState,
    senders: &AutoallocSenders,
) -> anyhow::Result<()> {
    for (queue_id, queue) in autoalloc.queues_mut() {
        try_pause_queue(queue, queue_id);
    }

    // Keep only active queues
    let queues = autoalloc
        .queues()
        .filter(|(_, queue)| queue.state().is_active())
        .collect::<Vec<_>>();
    if queues.is_empty() {
        return Ok(());
    }

    // If there is no allocation that we could possibly create, then stop
    if queues
        .iter()
        .all(|(_, queue)| !queue.has_space_for_submit())
    {
        return Ok(());
    }

    let queries: Vec<WorkerTypeQuery> = queues
        .iter()
        .map(|(id, queue)| {
            let query = create_queue_worker_query(queue);
            log::debug!("Creating worker query {query:?} for queue {id}");
            query
        })
        .collect();
    let responses = compute_query_responses(senders, queries)?;

    // Now schedule the workers into the individual queues
    let queue_ids: Vec<QueueId> = queues.into_iter().map(|(id, _)| id).collect();
    for (response, &queue_id) in responses.into_iter().zip(&queue_ids) {
        queue_try_submit(autoalloc, queue_id, senders, response).await;
    }

    for (queue_id, queue) in autoalloc.queues_mut() {
        try_pause_queue(queue, queue_id);
    }

    Ok(())
}

/// Create a worker query that corresponds to the provided resources of the given `queue`.
fn create_queue_worker_query(queue: &AllocationQueue) -> WorkerTypeQuery {
    let (descriptor, partial) = {
        if let Some(worker_resources) = queue.get_worker_resources() {
            // If we have known worker resources, we estimate exact resources based on it
            (worker_resources.clone(), false)
        } else if let Some(resources) = queue.info().cli_resource_descriptor() {
            // If not, try to at least estimate partial resources based from the CLI arguments
            (resources.clone(), true)
        } else {
            // Otherwise we cannot assume anything about the worker
            (ResourceDescriptor::new(vec![], Default::default()), true)
        }
    };

    let info = queue.info();
    WorkerTypeQuery {
        descriptor,
        partial,
        time_limit: Some(info.timelimit()),
        // How many workers can we provide at the moment
        max_sn_workers: info.backlog() * info.max_workers_per_alloc(),
        max_workers_per_allocation: info.max_workers_per_alloc(),
        min_utilization: info.min_utilization().unwrap_or(0.0),
    }
}

/// Computes the given worker queries and returns aggregated responses together with leftovers.
fn compute_query_responses(
    senders: &AutoallocSenders,
    queries: Vec<WorkerTypeQuery>,
) -> anyhow::Result<Vec<QueryResponse>> {
    let response = senders
        .server
        .new_worker_query(&queries)
        .context("cannot compute worker query")?;
    log::debug!("Scheduler query response: {response:?}");

    // Merge responses back into a single array
    let mut responses: Vec<QueryResponse> = response
        .single_node_workers_per_query
        .into_iter()
        .map(|single_node_workers| QueryResponse {
            single_node_workers,
            multinode_allocations: 0,
            multinode_workers_per_alloc: 0,
        })
        .collect();
    for mn_response in response.multi_node_allocations {
        let Some(response) = responses.get_mut(mn_response.worker_type) else {
            panic!(
                "Invalid queue index returned from worker query, response count: {}, index: {}",
                responses.len(),
                mn_response.worker_type
            );
        };
        response.multinode_allocations = mn_response.max_allocations;
        response.multinode_workers_per_alloc = mn_response.worker_per_allocation;
    }

    Ok(responses)
}

/// Permits the autoallocator to submit a given number of allocations with the given number of
/// workers, based on queue limits.
#[derive(Debug)]
struct SubmissionPermit {
    /// How many allocations should be submitted
    /// Each allocation holds the maximum number of workers that can be spawned in the allocation.
    allocs_to_submit: Vec<u64>,
}

impl SubmissionPermit {
    fn empty() -> Self {
        Self {
            allocs_to_submit: vec![],
        }
    }

    fn is_empty(&self) -> bool {
        self.allocs_to_submit.is_empty()
    }
}

/// Count how many submits can be performed on the given queue based on various factors.
fn compute_submission_permit(
    queue: &AllocationQueue,
    query_response: QueryResponse,
) -> SubmissionPermit {
    let info = queue.info();

    let active_workers = queue.active_worker_count();
    let queued_allocs = queue.queued_allocations().collect::<Vec<_>>();

    let mut max_remaining_workers_to_spawn = match info.max_worker_count() {
        Some(max) => max.saturating_sub(active_workers),
        None => u32::MAX,
    };

    log::debug!(
        "Counting possible allocations: query response {query_response:?}. Backlog: {}, currently queued: {}, max workers to spawn: {max_remaining_workers_to_spawn}",
        info.backlog(),
        queued_allocs.len()
    );

    if max_remaining_workers_to_spawn == 0 {
        return SubmissionPermit::empty();
    }

    // Step 1: filter out queued allocations that can fulfill the mn alloc request, and at the
    // same time filter our workers that satisfy the sn alloc request.
    let mut mn_alloc_count = query_response.multinode_allocations;
    let mut sn_worker_count = query_response.single_node_workers;

    for alloc in &queued_allocs {
        let mut worker_count = alloc.target_worker_count as u32;

        // This allocation already satisfies the mn request
        if mn_alloc_count > 0 && query_response.multinode_workers_per_alloc <= worker_count {
            mn_alloc_count -= 1;
            // Do not consider the mn workers to participate in the queued sn worker count
            worker_count -= query_response.multinode_workers_per_alloc;
        }
        // Subtract the number of sn workers that we need
        sn_worker_count = sn_worker_count.saturating_sub(worker_count);
    }

    // Step 2: create iterator of allocations to submit
    // Try to create as many workers per allocation as possible to satisfy sn workers
    let full_sn_alloc_count =
        ((sn_worker_count as f32) / (info.max_workers_per_alloc() as f32)).floor() as u32;
    let remainder = sn_worker_count % info.max_workers_per_alloc();
    let sn_allocs = (0..full_sn_alloc_count)
        .map(|_| info.max_workers_per_alloc())
        // Add the remainder to the iterator if it isn't empty
        .chain((remainder != 0).then_some(remainder));

    // Multi-node allocations have priority before single-node allocations
    let max_allocs_to_submit = info.backlog().saturating_sub(queued_allocs.len() as u32);
    let allocs_to_submit = (0..mn_alloc_count)
        .map(|_| query_response.multinode_workers_per_alloc)
        .chain(sn_allocs)
        .take(max_allocs_to_submit as usize);

    let mut allocations = vec![];
    for target_worker_count in allocs_to_submit {
        assert!(target_worker_count <= info.max_workers_per_alloc());
        let to_spawn = target_worker_count.min(max_remaining_workers_to_spawn);
        if to_spawn == 0 {
            break;
        }
        max_remaining_workers_to_spawn = max_remaining_workers_to_spawn.saturating_sub(to_spawn);
        allocations.push(to_spawn as u64);
    }

    log::debug!("Determined allocations to submit: {allocations:?}");

    SubmissionPermit {
        allocs_to_submit: allocations,
    }
}

/// Attempt to submit allocations so that up to a given `required_workers` number of workers is
/// queued.
async fn queue_try_submit(
    autoalloc: &mut AutoAllocState,
    queue_id: QueueId,
    senders: &AutoallocSenders,
    query_response: QueryResponse,
) {
    if query_response.is_empty() {
        return;
    }

    // Figure out how many allocations we are allowed to submit, according to queue limits
    // Also check the rate limiter
    let permit = {
        let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));
        if !queue.state().is_active() {
            return;
        }

        let permit = compute_submission_permit(queue, query_response);
        if permit.is_empty() {
            return;
        }

        let limiter = queue.limiter_mut();

        let status = limiter.submission_status();
        let allowed = matches!(status, RateLimiterStatus::Ok);
        if allowed {
            // Log a submission attempt, because we will try it below.
            // It is done here to avoid fetching the queue again.
            limiter.on_submission_attempt();
        } else {
            log::debug!("Submit attempt for queue {queue_id} was rate limited: {status:?}");
            return;
        }
        permit
    };

    for workers_to_spawn in permit.allocs_to_submit {
        let schedule_fut = {
            let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));
            let info = queue.info().clone();
            queue.handler_mut().submit_allocation(
                queue_id,
                &info,
                workers_to_spawn,
                SubmitMode::Submit,
            )
        };

        let result = schedule_fut.await;

        match result {
            Ok(submission_result) => {
                let working_dir = submission_result.working_dir().clone();
                match submission_result.into_id() {
                    Ok(allocation_id) => {
                        log::info!(
                            "Queued {workers_to_spawn} worker(s) into queue {queue_id}: allocation ID {allocation_id}"
                        );
                        senders.events.on_allocation_queued(
                            queue_id,
                            allocation_id.clone(),
                            workers_to_spawn,
                        );
                        let allocation =
                            Allocation::new(allocation_id, workers_to_spawn, working_dir);
                        autoalloc.add_allocation(allocation, queue_id);
                        let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));
                        queue.limiter_mut().on_submission_success();
                    }
                    Err(err) => {
                        log::error!("Failed to submit allocation into queue {queue_id}: {err:?}");
                        autoalloc.add_inactive_directory(working_dir);
                        let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));
                        queue.limiter_mut().on_submission_fail();
                        break;
                    }
                }
            }
            Err(err) => {
                log::error!("Failed to create allocation directory for queue {queue_id}: {err:?}");
                let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));
                queue.limiter_mut().on_submission_fail();
                break;
            }
        }
    }
}

fn create_rate_limiter() -> RateLimiter {
    RateLimiter::new(
        SUBMISSION_DELAYS.to_vec(),
        MAX_SUBMISSION_FAILS,
        max_allocation_fails(),
    )
}

fn create_queue(
    autoalloc: &mut AutoAllocState,
    events: &EventStreamer,
    server_directory: PathBuf,
    params: QueueParameters,
    queue_id: Option<QueueId>,
    worker_resources: Option<ResourceDescriptor>,
) -> anyhow::Result<QueueId> {
    let name = params.name.clone();
    let handler = create_allocation_handler(&params.manager, name.clone(), server_directory);
    let queue_info = QueueInfo::new(params.clone());

    match handler {
        Ok(handler) => {
            let queue = AllocationQueue::new(
                queue_info,
                name,
                handler,
                create_rate_limiter(),
                worker_resources,
            );
            let id = {
                let id = autoalloc.add_queue(queue, queue_id);
                if queue_id.is_none() {
                    // When queue_id is provided, then we are restoring journal,
                    // so we do not want to double log the event
                    events.on_allocation_queue_created(id, params);
                }
                id
            };

            Ok(id)
        }
        Err(error) => Err(anyhow::anyhow!("Could not create autoalloc queue: {error}")),
    }
}

// TODO: use proper error type
async fn remove_queue(
    autoalloc: &mut AutoAllocState,
    events: &EventStreamer,
    id: QueueId,
    force: bool,
) -> anyhow::Result<()> {
    let remove_alloc_fut = {
        let queue_state = autoalloc.get_queue_mut(id);

        let fut = match queue_state {
            Some(state) => {
                let has_running_allocations =
                    state.all_allocations().any(|alloc| alloc.is_running());
                if has_running_allocations && !force {
                    return Err(anyhow::anyhow!(
                        "Allocation queue has running jobs, so it will \
not be removed. Use `--force` if you want to remove the queue anyway"
                    ));
                }

                prepare_queue_cleanup(state)
            }
            None => return Err(anyhow::anyhow!("Allocation queue not found")),
        };

        autoalloc.remove_queue(id);
        fut
    };

    for (result, allocation_id) in futures::future::join_all(remove_alloc_fut).await {
        match result {
            Ok(_) => log::info!("Allocation {allocation_id} was removed"),
            Err(e) => log::error!("Failed to remove allocation {allocation_id}: {e:?}"),
        }
    }

    events.on_allocation_queue_removed(id);

    Ok(())
}

/// Removes all remaining active allocations
async fn stop_all_allocations(autoalloc: &AutoAllocState) {
    let futures = autoalloc
        .queues()
        .flat_map(|(_, queue)| prepare_queue_cleanup(queue));

    for (result, allocation_id) in futures::future::join_all(futures).await {
        match result {
            Ok(_) => {
                log::info!("Allocation {allocation_id} was removed");
            }
            Err(e) => {
                log::error!("Failed to remove allocation {allocation_id}: {e:?}");
            }
        }
    }
}

/// Synchronizes state for all queues with the external allocation manager and removes stale
/// directories from disk.
async fn do_periodic_update(senders: &AutoallocSenders, autoalloc: &mut AutoAllocState) {
    let queue_ids = autoalloc.queue_ids().collect::<Vec<_>>();
    for id in queue_ids {
        refresh_queue_allocations(&senders.events, autoalloc, id).await;
    }

    remove_inactive_directories(autoalloc).await;
}

fn get_data_from_worker<'a>(
    state: &'a mut AutoAllocState,
    manager_info: &ManagerInfo,
) -> Option<(&'a mut AllocationQueue, QueueId, AllocationId)> {
    let allocation_id = &manager_info.allocation_id;
    state
        .get_queue_id_by_allocation(allocation_id)
        .and_then(|queue_id| {
            state
                .get_queue_mut(queue_id)
                .map(|queue| (queue, queue_id, allocation_id.clone()))
        })
}

/// Synchronize the state of allocations with the external job manager.
async fn refresh_queue_allocations(
    events: &EventStreamer,
    autoalloc: &mut AutoAllocState,
    queue_id: QueueId,
) {
    log::debug!("Attempt to refresh allocations of queue {queue_id}");
    let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));

    let (status_fut, allocation_ids) = {
        let allocations: Vec<_> = queue.active_allocations().collect();
        if allocations.is_empty() {
            return;
        }

        let fut = queue.handler().get_status_of_allocations(&allocations);
        let allocation_ids: Vec<AllocationId> = allocations
            .into_iter()
            .map(|alloc| alloc.id.clone())
            .collect();
        (fut, allocation_ids)
    };

    let result = status_fut.await;

    log::debug!("Allocations of queue {queue_id} have been refreshed: {result:?}");

    match result {
        Ok(mut status_map) => {
            let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));
            log::debug!(
                "Active allocations of queue {queue_id} before update\n{:?}",
                queue.active_allocations().collect::<Vec<_>>()
            );
            for allocation_id in allocation_ids {
                let status = status_map.remove(&allocation_id).unwrap_or_else(|| {
                    Ok(AllocationExternalStatus::Failed {
                        started_at: None,
                        finished_at: AbsoluteTime::now(),
                    })
                });
                match status {
                    Ok(status) => {
                        sync_allocation_status(
                            events,
                            queue_id,
                            queue,
                            &allocation_id,
                            AllocationSyncReason::AllocationExternalChange(status),
                        );
                    }
                    Err(error) => {
                        log::warn!("Could not get status of allocation {allocation_id}: {error:?}");

                        // We failed to get a status of an allocation.
                        // This can either be a transient error, or it can mean that the manager
                        // will never give us a valid status ever again.
                        // To avoid the latter situation, we count the number of status errors that
                        // we saw, and if it reaches a threshold, we finish the allocation to let
                        // autoalloc submit new allocations instead.
                        if let Some(allocation) = queue.get_allocation_mut(&allocation_id) {
                            increase_status_error_counter(events, allocation, queue_id);
                        }
                    }
                }
            }
            log::debug!(
                "Active allocations of queue {queue_id} after update\n{:?}",
                queue.active_allocations().collect::<Vec<_>>()
            );
        }
        Err(error) => {
            log::error!("Failed to get allocations status from queue {queue_id}: {error:?}");
            // We could not even communicate with the allocation manager.
            // Pessimistically bump the status error counter for all active allocations of the queue
            for allocation in queue.active_allocations_mut() {
                increase_status_error_counter(events, allocation, queue_id);
            }
        }
    }
}

fn increase_status_error_counter(
    events: &EventStreamer,
    allocation: &mut Allocation,
    queue_id: QueueId,
) {
    match &mut allocation.status {
        AllocationState::Queued { status_error_count } => {
            *status_error_count += 1;
            if *status_error_count > MAX_QUEUED_STATUS_ERROR_COUNT {
                log::debug!(
                    "Queued allocation {} received too many status errors. It is now considered to be finished",
                    allocation.id
                );
                allocation.status = AllocationState::FinishedUnexpectedly {
                    connected_workers: Default::default(),
                    disconnected_workers: Default::default(),
                    started_at: None,
                    finished_at: AbsoluteTime::now(),
                    // The allocation did not receive any workers, and its
                    // status check has repeatedly failed, so most likely
                    // it has failed.
                    failed: true,
                };
                events.on_allocation_finished(queue_id, allocation.id.clone());
            }
        }
        AllocationState::Running {
            status_error_count,
            started_at,
            connected_workers,
            disconnected_workers,
        } => {
            *status_error_count += 1;
            if *status_error_count > MAX_RUNNING_STATUS_ERROR_COUNT {
                log::debug!(
                    "Running allocation {} received too many status errors. It is now considered to be finished",
                    allocation.id
                );
                allocation.status = AllocationState::FinishedUnexpectedly {
                    connected_workers: std::mem::take(connected_workers),
                    disconnected_workers: std::mem::take(disconnected_workers),
                    started_at: Some(*started_at),
                    finished_at: AbsoluteTime::now(),
                    // Hard to say what has happened, but since the
                    // allocation has at least started, consider it to be a
                    // success.
                    failed: false,
                };
                events.on_allocation_finished(queue_id, allocation.id.clone());
            }
        }
        AllocationState::Finished { .. } | AllocationState::FinishedUnexpectedly { .. } => {}
    }
}

#[derive(Debug)]
enum AllocationSyncReason {
    WorkedConnected(WorkerId),
    WorkerLost(WorkerId, LostWorkerDetails),
    AllocationExternalChange(AllocationExternalStatus),
}

/// Update the status of an allocation once an interesting event (`sync_reason`) has happened.
fn sync_allocation_status(
    events: &EventStreamer,
    queue_id: QueueId,
    queue: &mut AllocationQueue,
    allocation_id: &str,
    sync_reason: AllocationSyncReason,
) {
    let allocation = get_or_return!(queue.get_allocation_mut(allocation_id));

    log::debug!("Syncing status of allocation {allocation_id}, reason: {sync_reason:?}");

    #[derive(Copy, Clone, Debug)]
    enum AllocationFinished {
        AllWorkersFailed,
        AllWorkersFinished,
        FromQueuedToExternalFailure,
        FromQueuedToExternalFinish,
        FromRunningToExternalFailure,
        FromRunningToExternalFinish,
    }

    let finished: Option<AllocationFinished> = {
        match sync_reason {
            AllocationSyncReason::WorkedConnected(worker_id) => match &mut allocation.status {
                AllocationState::Queued { .. } => {
                    allocation.status = AllocationState::Running {
                        connected_workers: Set::from_iter([worker_id]),
                        disconnected_workers: Default::default(),
                        started_at: AbsoluteTime::now(),
                        status_error_count: 0,
                    };
                    events.on_allocation_started(queue_id, allocation_id.to_string());
                    None
                }
                AllocationState::Running {
                    connected_workers, ..
                } => {
                    if allocation.target_worker_count == connected_workers.len() as u64 {
                        log::warn!(
                            "Allocation {allocation_id} already has the expected number of workers, worker {worker_id} is not expected"
                        );
                    }
                    if !connected_workers.insert(worker_id) {
                        log::warn!(
                            "Allocation {allocation_id} already had worker {worker_id} connected"
                        );
                    }
                    None
                }
                AllocationState::Finished { .. } | AllocationState::FinishedUnexpectedly { .. } => {
                    log::warn!(
                        "Allocation {allocation_id} has status {:?} and does not expect new workers",
                        allocation.status
                    );
                    None
                }
            },
            AllocationSyncReason::WorkerLost(worker_id, details) => {
                match &mut allocation.status {
                    AllocationState::Queued {
                        status_error_count: _,
                    } => {
                        log::warn!(
                            "Worker {worker_id} has disconnected before its allocation was registered as running!"
                        );
                        None
                    }
                    AllocationState::Running {
                        disconnected_workers,
                        connected_workers,
                        started_at,
                        status_error_count: _,
                    } => {
                        if !connected_workers.remove(&worker_id) {
                            log::warn!(
                                "Worker {worker_id} has disconnected multiple times (or it has disconnected before connecting)!"
                            );
                        }
                        disconnected_workers.add_lost_worker(worker_id, details);

                        // All expected workers have disconnected, the allocation ends in a normal way
                        if disconnected_workers.count() == allocation.target_worker_count {
                            let is_failed = disconnected_workers.all_crashed();

                            allocation.status = AllocationState::Finished {
                                started_at: *started_at,
                                finished_at: AbsoluteTime::now(),
                                disconnected_workers: std::mem::take(disconnected_workers),
                            };

                            if is_failed {
                                Some(AllocationFinished::AllWorkersFailed)
                            } else {
                                Some(AllocationFinished::AllWorkersFinished)
                            }
                        } else {
                            None
                        }
                    }
                    AllocationState::Finished { .. } => {
                        log::warn!(
                            "Worker {worker_id} has disconnected from an already finished allocation {}",
                            allocation.id
                        );
                        None
                    }
                    AllocationState::FinishedUnexpectedly { .. } => {
                        log::warn!(
                            "Worker {worker_id} has disconnected from an invalid allocation {}",
                            allocation.id
                        );
                        None
                    }
                }
            }
            AllocationSyncReason::AllocationExternalChange(status) => {
                match (&mut allocation.status, status) {
                    (AllocationState::Queued { .. }, AllocationExternalStatus::Queued)
                    | (AllocationState::Running { .. }, AllocationExternalStatus::Running) => {
                        // No change
                        None
                    }
                    (AllocationState::Queued { .. }, AllocationExternalStatus::Running) => {
                        log::debug!(
                            "Allocation {allocation_id} marked externally as running, even though a worker has not connected yet"
                        );

                        // The allocation has started running. Usually, we will receive
                        // information about this by a worker being connected, but in theory this
                        // event can happen sooner (or the allocation can start, but the worker
                        // fails to start).
                        allocation.status = AllocationState::Running {
                            started_at: AbsoluteTime::now(),
                            connected_workers: Default::default(),
                            disconnected_workers: Default::default(),
                            status_error_count: 0,
                        };
                        None
                    }
                    (
                        AllocationState::Queued { .. },
                        ref status @ (AllocationExternalStatus::Finished {
                            finished_at,
                            started_at,
                        }
                        | AllocationExternalStatus::Failed {
                            finished_at,
                            started_at,
                        }),
                    ) => {
                        // The allocation has finished without any worker connecting.
                        // This is an unexpected situation.
                        let failed = matches!(status, AllocationExternalStatus::Failed { .. });

                        allocation.status = AllocationState::FinishedUnexpectedly {
                            connected_workers: Default::default(),
                            disconnected_workers: Default::default(),
                            started_at,
                            finished_at,
                            failed,
                        };

                        Some(if failed {
                            AllocationFinished::FromQueuedToExternalFailure
                        } else {
                            AllocationFinished::FromQueuedToExternalFinish
                        })
                    }
                    (
                        AllocationState::Running {
                            disconnected_workers,
                            started_at: _,
                            connected_workers,
                            status_error_count: _,
                        },
                        ref status @ (AllocationExternalStatus::Finished {
                            started_at,
                            finished_at,
                        }
                        | AllocationExternalStatus::Failed {
                            started_at,
                            finished_at,
                        }),
                    ) => {
                        // The allocation has finished without the last worker disconnecting.
                        // This usually shouldn't happen, but we still try to deal with it.
                        let failed = matches!(status, AllocationExternalStatus::Failed { .. });

                        allocation.status = AllocationState::FinishedUnexpectedly {
                            connected_workers: std::mem::take(connected_workers),
                            disconnected_workers: std::mem::take(disconnected_workers),
                            started_at,
                            finished_at,
                            failed,
                        };

                        Some(if failed {
                            AllocationFinished::FromRunningToExternalFailure
                        } else {
                            AllocationFinished::FromRunningToExternalFinish
                        })
                    }
                    (
                        AllocationState::Finished { .. }
                        | AllocationState::FinishedUnexpectedly { .. },
                        status,
                    ) => {
                        // The allocation was already finished before, we do not expect any more
                        // status updates.
                        log::debug!(
                            "Unexpected external allocation {allocation_id} status {status:?}, the allocation was already finished before."
                        );
                        None
                    }
                    (AllocationState::Running { .. }, AllocationExternalStatus::Queued) => {
                        // A nonsensical transform that should not ever happen. But the allocation
                        // manager can have weird caching, so we at least log something.
                        log::debug!(
                            "Unexpected external allocation {allocation_id} status queued, the allocation was already running."
                        );
                        None
                    }
                }
            }
        }
    };

    match finished {
        Some(
            AllocationFinished::AllWorkersFinished
            | AllocationFinished::FromQueuedToExternalFinish
            | AllocationFinished::FromRunningToExternalFinish,
        ) => {
            log::debug!("Marking allocation {allocation_id} as success: {finished:?}");
            queue.limiter_mut().on_allocation_success();
            events.on_allocation_finished(queue_id, allocation_id.to_string());
        }
        Some(
            AllocationFinished::AllWorkersFailed
            | AllocationFinished::FromQueuedToExternalFailure
            | AllocationFinished::FromRunningToExternalFailure,
        ) => {
            log::debug!("Marking allocation {allocation_id} as failure: {finished:?}");
            queue.limiter_mut().on_allocation_fail();
            events.on_allocation_finished(queue_id, allocation_id.to_string());
        }
        None => {}
    }
}

async fn remove_inactive_directories(autoalloc: &mut AutoAllocState) {
    let to_remove = autoalloc.get_directories_for_removal();
    let futures = to_remove.into_iter().map(|dir| async move {
        let result = tokio::fs::remove_dir_all(&dir).await;
        (result, dir)
    });
    for (result, directory) in join_all(futures).await {
        if let Err(err) = result {
            log::error!("Failed to remove stale allocation directory {directory:?}: {err:?}");
        }
    }
}

/// Try to pause a queue automatically.
/// Returns true if the queue is paused after this function finishes.
fn try_pause_queue(queue: &mut AllocationQueue, id: QueueId) -> bool {
    if !queue.state().is_active() {
        return true;
    }
    let limiter = queue.limiter();

    let status = limiter.submission_status();
    match status {
        RateLimiterStatus::TooManyFailedSubmissions => {
            log::error!("The queue {id} had too many failed submissions, it will be paused.");
            queue.pause();
            true
        }
        RateLimiterStatus::TooManyFailedAllocations => {
            log::error!("The queue {id} had too many failed allocations, it will be paused.");
            queue.pause();
            true
        }
        RateLimiterStatus::Ok | RateLimiterStatus::Wait => false,
    }
}

pub fn prepare_queue_cleanup(
    state: &AllocationQueue,
) -> Vec<impl Future<Output = (AutoAllocResult<()>, AllocationId)> + use<>> {
    let handler = state.handler();
    state
        .active_allocations()
        .map(move |alloc| {
            let allocation_id = alloc.id.clone();
            let future = handler.remove_allocation(alloc);
            async move { (future.await, allocation_id) }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;

    use std::time::{Duration, Instant};

    use crate::common::manager::info::{
        GetManagerInfo, ManagerInfo, ManagerType, WORKER_EXTRA_MANAGER_KEY,
    };
    use crate::common::rpc::ResponseToken;
    use crate::common::utils::time::mock_time::MockTime;
    use crate::server::autoalloc::process::{
        AutoallocSenders, QueryResponse, compute_submission_permit, do_periodic_update,
        handle_message, perform_submits,
    };
    use crate::server::autoalloc::queue::{
        AllocationExternalStatus, AllocationStatusMap, AllocationSubmissionResult, QueueHandler,
        SubmitMode,
    };
    use crate::server::autoalloc::service::AutoAllocMessage;
    use crate::server::autoalloc::state::{
        AllocationQueue, AllocationQueueState, AllocationState, AutoAllocState, RateLimiter,
    };
    use crate::server::autoalloc::{
        Allocation, AllocationId, AutoAllocResult, LostWorkerDetails, QueueId, QueueInfo,
        QueueParameters,
    };
    use crate::server::event::streamer::EventStreamer;
    use anyhow::anyhow;
    use derive_builder::Builder;
    use log::LevelFilter;
    use tako::WorkerId;
    use tako::gateway::LostWorkerReason;
    use tako::resources::ResourceDescriptor;
    use tako::tests::integration::utils::api::wait_for_worker_connected;
    use tako::tests::integration::utils::server::{
        ServerConfigBuilder, ServerHandle, run_server_test,
    };
    use tako::tests::integration::utils::task::{
        GraphBuilder, ResourceRequestConfigBuilder, TaskConfigBuilder, simple_args,
    };
    use tako::tests::integration::utils::worker::{
        WorkerConfigBuilder, create_worker_configuration,
    };
    use tako::worker::WorkerConfiguration;
    use tako::{Map, Set, WrappedRcRefCell};
    use tempfile::TempDir;

    #[test]
    fn permit_respect_backlog() {
        let queue = QueueBuilder::default().backlog(2);
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue.into(),
            query_response_sn(3),
        ).allocs_to_submit, @r"
        [
            1,
            1,
        ]
        ");
    }

    #[test]
    fn permit_less_than_max_worker_count_per_alloc() {
        let queue = QueueBuilder::default().backlog(4).max_workers_per_alloc(2);
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue.into(),
            query_response_sn(1),
        ).allocs_to_submit, @r"
        [
            1,
        ]
        ");
    }

    #[test]
    fn permit_respect_max_workers() {
        let queue = QueueBuilder::default().backlog(4).max_worker_count(Some(3));
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue.into(),
            query_response_sn(4),
        ).allocs_to_submit, @r"
        [
            1,
            1,
            1,
        ]
        ");
    }

    #[test]
    fn permit_respect_max_workers_with_more_workers_per_node() {
        let queue = QueueBuilder::default()
            .backlog(4)
            .max_workers_per_alloc(2)
            .max_worker_count(Some(3));
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue.into(),
            query_response_sn(4),
        ).allocs_to_submit, @r"
        [
            2,
            1,
        ]
        ");
    }

    #[test]
    fn permit_respect_queued_allocations_empty() {
        let mut queue = QueueBuilder::default().backlog(4).into();
        add_alloc(&mut queue, 0, 1);
        add_alloc(&mut queue, 1, 1);
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue,
            query_response_sn(2),
        ).allocs_to_submit, @"[]");
    }

    #[test]
    fn permit_respect_queued_mn_allocations_1() {
        let mut queue = QueueBuilder::default()
            .backlog(4)
            .max_workers_per_alloc(3)
            .into();
        // This alloc has enough workers
        add_alloc(&mut queue, 0, 3);
        // This one doesn't, so an additional alloc should be created
        add_alloc(&mut queue, 1, 1);
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue,
            QueryResponse {
                single_node_workers: 0,
                multinode_allocations: 2,
                multinode_workers_per_alloc: 2,
            },
        ).allocs_to_submit, @r"
        [
            2,
        ]
        ");
    }

    #[test]
    fn permit_respect_queued_mn_allocations_2() {
        let mut queue = QueueBuilder::default().backlog(4).into();
        // Both allocs have enough workers, do not submit anything else
        add_alloc(&mut queue, 0, 3);
        add_alloc(&mut queue, 1, 2);
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue,
            QueryResponse {
                single_node_workers: 0,
                multinode_allocations: 2,
                multinode_workers_per_alloc: 2,
            },
        ).allocs_to_submit, @r"
        []
        ");
    }

    #[test]
    fn permit_mn_allocation_simple() {
        let queue = QueueBuilder::default()
            .backlog(4)
            .max_workers_per_alloc(2)
            .into();
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue,
            QueryResponse {
                single_node_workers: 0,
                multinode_allocations: 2,
                multinode_workers_per_alloc: 2,
            },
        ).allocs_to_submit, @r"
        [
            2,
            2,
        ]
        ");
    }

    #[test]
    fn permit_mn_and_sn_allocation() {
        let queue = QueueBuilder::default()
            .backlog(4)
            .max_workers_per_alloc(2)
            .into();
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue,
            QueryResponse {
                single_node_workers: 3,
                multinode_allocations: 2,
                multinode_workers_per_alloc: 2,
            },
        ).allocs_to_submit, @r"
        [
            2,
            2,
            2,
            1,
        ]
        ");
    }

    #[test]
    fn permit_allocation_satisfies_both_mn_and_sn_1() {
        let mut queue = QueueBuilder::default()
            .backlog(5)
            .max_workers_per_alloc(2)
            .into();
        // This allocation satisfies one mn allocation and one sn worker
        add_alloc(&mut queue, 0, 3);
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue,
            QueryResponse {
                single_node_workers: 4,
                multinode_allocations: 2,
                multinode_workers_per_alloc: 2,
            },
        ).allocs_to_submit, @r"
        [
            2,
            2,
            1,
        ]
        ");
    }

    #[test]
    fn permit_allocation_satisfies_both_mn_and_sn_2() {
        let mut queue = QueueBuilder::default()
            .backlog(4)
            .max_workers_per_alloc(3)
            .into();
        // This allocation satisfies one mn allocation and one sn worker
        add_alloc(&mut queue, 0, 3);
        // This allocation satisfies three sn workers
        add_alloc(&mut queue, 1, 3);
        insta::assert_debug_snapshot!(compute_submission_permit(
            &queue,
            QueryResponse {
                single_node_workers: 4,
                multinode_allocations: 1,
                multinode_workers_per_alloc: 2,
            },
        ).allocs_to_submit, @"[]");
    }

    #[tokio::test]
    async fn fill_backlog() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(4).max_workers_per_alloc(2),
                )
                .await;

            ctx.create_simple_tasks(100).await;
            ctx.assign_worker_resource(queue_id, WorkerConfigBuilder::default());
            ctx.try_submit().await;

            let allocations = ctx.get_allocations(queue_id);
            assert_eq!(allocations.len(), 4);
            assert!(
                allocations
                    .iter()
                    .all(|alloc| alloc.target_worker_count == 2)
            );
        })
        .await;
    }

    // When we don't know worker resources, create only a single conservative "probe" allocation
    // if there are some tasks waiting for resources
    #[tokio::test]
    async fn unknown_worker_resources() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(4).max_workers_per_alloc(2),
                )
                .await;

            ctx.create_simple_tasks(100).await;
            ctx.try_submit().await;

            let allocations = ctx.get_allocations(queue_id);
            // Create only a single "probe" conservative allocation
            assert_eq!(allocations.len(), 1);
            assert!(
                allocations
                    .iter()
                    .all(|alloc| alloc.target_worker_count == 1)
            );
        })
        .await;
    }

    // Check that we create a probe allocation for each queue
    #[tokio::test]
    async fn unknown_worker_resources_multi_queue() {
        run_test(async |mut ctx: TestCtx| {
            let mut queues = vec![];
            for _ in 0..2 {
                queues.push(
                    ctx.add_queue(always_queued_handler(), QueueBuilder::default())
                        .await,
                );
            }

            // Note: we currently create an allocation per queue even if the task count is smaller
            // than the queue count. Could be improved in the future.
            ctx.create_simple_tasks(1).await;
            ctx.try_submit().await;

            for queue_id in queues {
                assert_eq!(ctx.get_allocations(queue_id).len(), 1);
            }
        })
        .await;
    }

    // Check that we won't create an allocation if we know that a worker couldn't handle it,
    // even if no worker has connected yet.
    #[tokio::test]
    async fn partial_worker_resources() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    // Expect that worker will have 2 CPUs
                    QueueBuilder::default().cli_resources(Some(ResourceDescriptor::simple_cpus(2))),
                )
                .await;

            // Create 4 CPU core tasks
            ctx.handle
                .submit(
                    GraphBuilder::default()
                        .task(
                            TaskConfigBuilder::default()
                                .resources(ResourceRequestConfigBuilder::default().cpus(4)),
                        )
                        .build(),
                )
                .await;
            ctx.try_submit().await;

            assert!(ctx.get_allocations(queue_id).is_empty());
        })
        .await;
    }

    // Check that autoalloc reacts to a worker connecting from an allocation, which should update
    // the known queue resources and allow more precise allocation submissions.
    #[tokio::test]
    async fn react_to_new_resources() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(always_queued_handler(), QueueBuilder::default().backlog(4))
                .await;

            ctx.create_simple_tasks(1000).await;

            // Create a single allocation
            ctx.try_submit().await;
            let allocations = ctx.get_allocations(queue_id);
            assert_eq!(allocations.len(), 1);

            // Connect worker, which shows autoalloc the available resources
            ctx.start_worker(WorkerConfigBuilder::default(), allocations[0].id.as_str())
                .await;

            // Now submit again and check that more allocations were created
            ctx.try_submit().await;
            assert_eq!(ctx.get_allocations(queue_id).len(), 5);
        })
        .await;
    }

    #[tokio::test]
    async fn do_nothing_on_full_backlog() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(4).max_workers_per_alloc(1),
                )
                .await;

            ctx.assign_worker_resource(queue_id, WorkerConfigBuilder::default());
            ctx.create_simple_tasks(100).await;

            for _ in 0..5 {
                ctx.try_submit().await;
            }

            assert_eq!(ctx.get_allocations(queue_id).len(), 4);
        })
        .await;
    }

    #[tokio::test]
    async fn worker_connects_from_unknown_allocation() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(1).max_workers_per_alloc(1),
                )
                .await;

            ctx.create_simple_tasks(100).await;
            ctx.try_submit().await;
            // Worker from an unknown allocation
            ctx.start_worker(WorkerConfigBuilder::default(), "foo")
                .await;

            assert!(
                ctx.get_allocations(queue_id)
                    .iter()
                    .all(|alloc| !alloc.is_running())
            );
        })
        .await;
    }

    #[tokio::test]
    async fn start_allocation_when_worker_connects() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(1).max_workers_per_alloc(1),
                )
                .await;

            ctx.create_simple_tasks(1).await;
            ctx.try_submit().await;

            let allocations = ctx.get_allocations(queue_id);
            let worker_id = ctx
                .start_worker(WorkerConfigBuilder::default(), allocations[0].id.as_str())
                .await;

            ctx.check_running_workers(&ctx.get_allocations(queue_id)[0], vec![worker_id]);
        })
        .await;
    }

    #[tokio::test]
    async fn add_another_worker_to_allocation() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(1).max_workers_per_alloc(2),
                )
                .await;

            ctx.create_simple_tasks(100).await;
            ctx.try_submit().await;

            let allocations = ctx.get_allocations(queue_id);
            let w0 = ctx
                .start_worker(WorkerConfigBuilder::default(), allocations[0].id.as_str())
                .await;
            let w1 = ctx
                .start_worker(WorkerConfigBuilder::default(), allocations[0].id.as_str())
                .await;

            ctx.check_running_workers(&ctx.get_allocations(queue_id)[0], vec![w0, w1]);
        })
        .await;
    }

    #[tokio::test]
    async fn finish_allocation_when_worker_disconnects() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(1).max_workers_per_alloc(1),
                )
                .await;

            ctx.create_simple_tasks(100).await;
            ctx.try_submit().await;

            let w0 = ctx
                .start_worker(
                    WorkerConfigBuilder::default(),
                    ctx.get_allocations(queue_id)[0].id.as_str(),
                )
                .await;
            ctx.stop_worker(w0, lost_worker_normal(LostWorkerReason::ConnectionLost))
                .await;
            ctx.check_finished_workers(
                &ctx.get_allocations(queue_id)[0],
                vec![(w0, LostWorkerReason::ConnectionLost)],
            );
        })
        .await;
    }

    #[tokio::test]
    async fn finish_allocation_when_last_worker_disconnects() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(1).max_workers_per_alloc(2),
                )
                .await;

            ctx.assign_worker_resource(queue_id, WorkerConfigBuilder::default());
            ctx.create_simple_tasks(100).await;
            ctx.try_submit().await;

            let allocations = ctx.get_allocations(queue_id);
            let w0 = ctx
                .start_worker(WorkerConfigBuilder::default(), allocations[0].id.as_str())
                .await;
            let w1 = ctx
                .start_worker(WorkerConfigBuilder::default(), allocations[0].id.as_str())
                .await;

            ctx.stop_worker(w0, lost_worker_normal(LostWorkerReason::ConnectionLost))
                .await;
            ctx.check_running_workers(&ctx.get_allocations(queue_id)[0], vec![w1]);

            ctx.stop_worker(w1, lost_worker_normal(LostWorkerReason::HeartbeatLost))
                .await;
            ctx.check_finished_workers(
                &ctx.get_allocations(queue_id)[0],
                vec![
                    (w0, LostWorkerReason::ConnectionLost),
                    (w1, LostWorkerReason::HeartbeatLost),
                ],
            );
        })
        .await;
    }

    #[tokio::test]
    async fn do_not_create_allocations_without_tasks() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(4).max_workers_per_alloc(2),
                )
                .await;
            ctx.try_submit().await;
            assert!(ctx.get_allocations(queue_id).is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn do_not_fill_backlog_when_tasks_run_out() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(5).max_workers_per_alloc(2),
                )
                .await;

            ctx.assign_worker_resource(queue_id, WorkerConfigBuilder::default());
            ctx.create_simple_tasks(5).await;
            ctx.try_submit().await;

            // 5 tasks, 3 * 2 workers -> last two allocations should be ignored
            assert_eq!(ctx.get_allocations(queue_id).len(), 3);
        })
        .await;
    }

    #[tokio::test]
    async fn stop_allocating_on_error() {
        run_test(async |mut ctx: TestCtx| {
            let handler_state = WrappedRcRefCell::wrap(HandlerState::default());
            let queue_id = ctx
                .add_queue(
                    stateful_handler(handler_state.clone()),
                    QueueBuilder::default().backlog(5).max_workers_per_alloc(1),
                )
                .await;

            ctx.assign_worker_resource(queue_id, WorkerConfigBuilder::default());
            ctx.create_simple_tasks(5).await;

            handler_state.get_mut().allocation_will_fail = true;

            // Only try the first allocation in the backlog
            ctx.try_submit().await;
            assert_eq!(handler_state.get().allocation_attempts, 1);

            handler_state.get_mut().allocation_will_fail = false;

            // Finish the rest
            ctx.try_submit().await;
            assert_eq!(handler_state.get().allocation_attempts, 6);
        })
        .await;
    }

    #[tokio::test]
    async fn ignore_task_with_high_time_request() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().timelimit(Duration::from_secs(60 * 30)),
                )
                .await;
            ctx.handle
                .submit(
                    GraphBuilder::default()
                        .task(
                            TaskConfigBuilder::default().resources(
                                ResourceRequestConfigBuilder::default()
                                    .cpus(1)
                                    .min_time(Duration::from_secs(60 * 60)),
                            ),
                        )
                        .build(),
                )
                .await;

            // Allocations last for 30 minutes, but the task requires 60 minutes
            // Nothing should be scheduled
            ctx.try_submit().await;
            assert_eq!(ctx.get_allocations(queue_id).len(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn respect_max_worker_count() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(4).max_worker_count(Some(5)),
                )
                .await;

            ctx.assign_worker_resource(queue_id, WorkerConfigBuilder::default());
            ctx.create_simple_tasks(100).await;

            // Put 4 allocations into the queue.
            ctx.try_submit().await;
            let allocations = ctx.get_allocations(queue_id);
            assert_eq!(allocations.len(), 4);

            // Start 2 allocations
            ctx.start_worker(WorkerConfigBuilder::default(), allocations[0].id.as_str())
                .await;
            let w1 = ctx
                .start_worker(WorkerConfigBuilder::default(), allocations[1].id.as_str())
                .await;

            // Create only one additional allocation
            ctx.try_submit().await;
            assert_eq!(ctx.get_allocations(queue_id).len(), 5);

            ctx.stop_worker(w1, lost_worker_normal(LostWorkerReason::ConnectionLost))
                .await;

            // One worker was freed, create an additional allocation
            ctx.try_submit().await;
            assert_eq!(ctx.get_allocations(queue_id).len(), 6);
        })
        .await;
    }

    #[tokio::test]
    async fn max_worker_count_shorten_last_allocation() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default()
                        .backlog(2)
                        .max_workers_per_alloc(4)
                        .max_worker_count(Some(6)),
                )
                .await;

            ctx.assign_worker_resource(queue_id, WorkerConfigBuilder::default());
            ctx.create_simple_tasks(100).await;

            ctx.try_submit().await;
            let allocations = ctx.get_allocations(queue_id);
            assert_eq!(allocations.len(), 2);
            assert_eq!(allocations[0].target_worker_count, 4);
            assert_eq!(allocations[1].target_worker_count, 2);
        })
        .await;
    }

    #[tokio::test]
    async fn delete_stale_directories_of_unsubmitted_allocations() {
        run_test(async |mut ctx: TestCtx| {
            let make_dir = || {
                let tempdir = TempDir::with_prefix("hq").unwrap();
                tempdir.keep()
            };

            let max_kept = 2;
            ctx.state.set_max_kept_directories(max_kept);
            ctx.add_queue(fails_submit_handler(), QueueBuilder::default())
                .await;
            ctx.create_simple_tasks(100).await;

            let dirs = [make_dir(), make_dir()];
            ctx.state
                .set_inactive_allocation_directories(dirs.iter().cloned().collect());

            // Delete oldest directory
            ctx.try_submit().await;
            ctx.periodic_update().await;
            assert!(!dirs[0].exists());
            assert!(dirs[1].exists());

            // Delete second oldest directory
            ctx.try_submit().await;
            ctx.periodic_update().await;
            assert!(!dirs[1].exists());
        })
        .await;
    }

    #[tokio::test]
    async fn pause_queue_when_submission_fails_too_many_times() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    fails_submit_handler(),
                    QueueBuilder::default()
                        .backlog(5)
                        .limiter_max_submit_fails(2),
                )
                .await;
            ctx.create_simple_tasks(100).await;

            ctx.try_submit().await;
            ctx.check_queue_status(queue_id, AllocationQueueState::Active);
            ctx.try_submit().await;
            ctx.check_queue_status(queue_id, AllocationQueueState::Paused);
        })
        .await;
    }

    #[tokio::test]
    async fn pause_queue_when_allocation_fails_too_many_times() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default()
                        .backlog(5)
                        .limiter_max_alloc_fails(2),
                )
                .await;

            ctx.assign_worker_resource(queue_id, WorkerConfigBuilder::default());
            ctx.create_simple_tasks(100).await;
            ctx.try_submit().await;

            let allocations = ctx.get_allocations(queue_id);
            ctx.fail_allocation_quick(&allocations[0]).await;
            ctx.try_submit().await;
            ctx.check_queue_status(queue_id, AllocationQueueState::Active);

            ctx.fail_allocation_quick(&allocations[1]).await;

            let allocations = ctx.get_allocations(queue_id);
            ctx.try_submit().await;
            ctx.check_queue_status(queue_id, AllocationQueueState::Paused);

            // Make sure the try_submit call above did not actually submit anything
            assert_eq!(ctx.get_allocations(queue_id).len(), allocations.len());
        })
        .await;
    }

    #[tokio::test]
    async fn respect_rate_limiter() {
        run_test(async |mut ctx: TestCtx| {
            let shared = WrappedRcRefCell::wrap(HandlerState::default());
            ctx.add_queue(
                stateful_handler(shared.clone()),
                QueueBuilder::default()
                    .backlog(5)
                    .limiter_max_alloc_fails(100)
                    .limiter_delays(vec![
                        Duration::ZERO,
                        Duration::from_secs(1),
                        Duration::from_secs(10),
                    ]),
            )
            .await;
            ctx.create_simple_tasks(100).await;

            shared.get_mut().allocation_will_fail = true;

            let check_alloc_count = |count: usize| {
                assert_eq!(shared.get().allocation_attempts, count);
            };
            let mut now = Instant::now();
            {
                let _mock = MockTime::mock(now);
                ctx.try_submit().await;
                check_alloc_count(1);
            }

            // Now we should sleep until +1 sec
            {
                now += Duration::from_millis(500);
                let _mock = MockTime::mock(now);
                ctx.try_submit().await;
                check_alloc_count(1);
            }
            {
                now += Duration::from_millis(1500);
                let _mock = MockTime::mock(now);
                ctx.try_submit().await;
                check_alloc_count(2);
            }

            // Now we should sleep until +10 sec
            {
                now += Duration::from_millis(5000);
                let _mock = MockTime::mock(now);
                ctx.try_submit().await;
                check_alloc_count(2);
            }
            {
                now += Duration::from_millis(6000);
                let _mock = MockTime::mock(now);
                ctx.try_submit().await;
                check_alloc_count(3);
            }
            // The delay shouldn't increase any more when we have reached the maximum delay
            {
                now += Duration::from_millis(11000);
                let _mock = MockTime::mock(now);
                ctx.try_submit().await;
                check_alloc_count(4);
            }
        })
        .await;
    }

    #[tokio::test]
    async fn bump_fail_counter_if_worker_fails_quick() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(1).max_workers_per_alloc(1),
                )
                .await;
            ctx.create_simple_tasks(1000).await;

            ctx.try_submit().await;
            let allocations = ctx.get_allocations(queue_id);

            ctx.fail_allocation_quick(&allocations[0]).await;
            assert_eq!(
                ctx.state
                    .get_queue(queue_id)
                    .unwrap()
                    .limiter()
                    .allocation_fail_count(),
                1
            );
        })
        .await;
    }

    #[tokio::test]
    async fn do_not_bump_fail_counter_if_worker_fails_normally() {
        run_test(async |mut ctx: TestCtx| {
            let queue_id = ctx
                .add_queue(
                    always_queued_handler(),
                    QueueBuilder::default().backlog(1).max_workers_per_alloc(1),
                )
                .await;
            ctx.create_simple_tasks(1000).await;

            ctx.try_submit().await;
            let allocations = ctx.get_allocations(queue_id);

            let w0 = ctx
                .start_worker(WorkerConfigBuilder::default(), allocations[0].id.as_str())
                .await;
            ctx.stop_worker(w0, lost_worker_normal(LostWorkerReason::ConnectionLost))
                .await;
            assert_eq!(
                ctx.state
                    .get_queue(queue_id)
                    .unwrap()
                    .limiter()
                    .allocation_fail_count(),
                0
            );
        })
        .await;
    }

    // Utilities
    struct TestCtx {
        state: AutoAllocState,
        senders: AutoallocSenders,
        handle: ServerHandle,
        workers: Map<WorkerId, WorkerConfiguration>,
    }

    impl TestCtx {
        async fn add_queue(
            &mut self,
            handler: Box<dyn QueueHandler>,
            queue_builder: QueueBuilder,
        ) -> QueueId {
            let (mut params, rate_limiter) = queue_builder.build();
            params.name = Some("Queue".to_string());

            let (token, rx) = ResponseToken::new();
            handle_message(
                &mut self.state,
                &self.senders.events,
                AutoAllocMessage::AddQueue {
                    server_directory: PathBuf::from("test"),
                    params,
                    queue_id: None,
                    worker_resources: None,
                    response: token,
                },
            )
            .await;
            let queue_id = rx.await.unwrap().unwrap();

            // A bit hacky, but easier than creating a mock for an external service, as we do in
            // Python.
            let queue = self.state.get_queue_mut(queue_id).unwrap();
            queue.set_handler(handler);
            *queue.limiter_mut() = rate_limiter;

            queue_id
        }

        async fn try_submit(&mut self) {
            perform_submits(&mut self.state, &self.senders)
                .await
                .unwrap();
        }

        async fn create_simple_tasks(&mut self, count: u64) {
            self.handle
                .submit(
                    GraphBuilder::default()
                        .task_copied(
                            TaskConfigBuilder::default().args(simple_args(&["ls"])),
                            count,
                        )
                        .build(),
                )
                .await;
        }

        fn assign_worker_resource(&mut self, queue: QueueId, worker: WorkerConfigBuilder) {
            let config = create_worker_configuration(worker).0;
            self.state
                .get_queue_mut(queue)
                .expect("queue not found")
                .register_worker_resources(config);
        }

        async fn start_worker<Info: Into<ManagerInfo>>(
            &mut self,
            worker: WorkerConfigBuilder,
            info: Info,
        ) -> WorkerId {
            let info = info.into();
            let handle = self
                .handle
                .start_worker(worker.with_manager_info(info.clone()))
                .await
                .expect("cannot start worker");
            let config = wait_for_worker_connected(&mut self.handle, handle.id).await;
            assert_eq!(
                info,
                config.get_manager_info().expect("missing manager info")
            );
            assert!(
                handle_message(
                    &mut self.state,
                    &self.senders.events,
                    AutoAllocMessage::WorkerConnected {
                        id: handle.id,
                        config: config.clone(),
                        manager_info: info
                    }
                )
                .await
            );

            assert!(self.workers.insert(handle.id, config).is_none());

            handle.id
        }

        async fn stop_worker(&mut self, id: WorkerId, details: LostWorkerDetails) {
            self.handle.stop_worker(id).await;
            let config = self.workers.get(&id).unwrap();
            let info = config.get_manager_info().unwrap();
            assert!(
                handle_message(
                    &mut self.state,
                    &self.senders.events,
                    AutoAllocMessage::WorkerLost(id, info, details)
                )
                .await
            );
        }

        async fn periodic_update(&mut self) {
            do_periodic_update(&self.senders, &mut self.state).await;
        }

        async fn fail_allocation_quick(&mut self, allocation: &Allocation) {
            let w0 = self
                .start_worker(WorkerConfigBuilder::default(), allocation.id.as_str())
                .await;
            self.stop_worker(w0, lost_worker_quick(LostWorkerReason::ConnectionLost))
                .await;
        }

        fn get_allocations(&self, id: QueueId) -> Vec<Allocation> {
            let mut allocations: Vec<_> = self
                .state
                .get_queue(id)
                .unwrap()
                .all_allocations()
                .cloned()
                .collect();
            allocations.sort_by(|a, b| a.id.cmp(&b.id));
            allocations
        }

        fn check_running_workers(&self, allocation: &Allocation, workers: Vec<WorkerId>) {
            match &allocation.status {
                AllocationState::Running {
                    connected_workers, ..
                } => {
                    let mut connected: Vec<WorkerId> = connected_workers.iter().cloned().collect();
                    connected.sort_unstable();
                    assert_eq!(connected, workers);
                }
                _ => panic!("Allocation {allocation:?} should be in running status"),
            }
        }

        fn check_finished_workers(
            &self,
            allocation: &Allocation,
            workers: Vec<(WorkerId, LostWorkerReason)>,
        ) {
            match &allocation.status {
                AllocationState::Finished {
                    disconnected_workers,
                    ..
                } => {
                    let mut disconnected: Vec<_> = disconnected_workers
                        .clone()
                        .into_iter()
                        .map(|(id, details)| (id, details.reason))
                        .collect();
                    disconnected.sort_unstable_by_key(|(id, _)| *id);
                    assert_eq!(disconnected, workers);
                }
                _ => panic!("Allocation should be in finished status"),
            }
        }

        fn check_queue_status(&self, id: QueueId, state: AllocationQueueState) {
            assert_eq!(*self.state.get_queue(id).unwrap().state(), state);
        }
    }

    async fn run_test<F: AsyncFnOnce(TestCtx)>(f: F) {
        let _ = env_logger::Builder::default()
            .filter(Some("hyperqueue::server::autoalloc"), LevelFilter::Debug)
            .is_test(true)
            .try_init();

        let state = AutoAllocState::new(1);
        run_server_test(ServerConfigBuilder::default(), |handle| async move {
            let server_ref = handle.server_ref.clone();
            let ctx = TestCtx {
                state,
                senders: AutoallocSenders {
                    server: server_ref,
                    events: EventStreamer::new(None),
                },
                handle,
                workers: Default::default(),
            };
            f(ctx).await;
        })
        .await;
    }

    trait WorkerConfigBuilderExt {
        fn with_manager_info(self, info: ManagerInfo) -> Self;
    }

    impl WorkerConfigBuilderExt for WorkerConfigBuilder {
        fn with_manager_info(self, info: ManagerInfo) -> Self {
            self.extra({
                let mut map = Map::default();
                map.insert(
                    WORKER_EXTRA_MANAGER_KEY.to_string(),
                    serde_json::to_string(&info).unwrap(),
                );
                map
            })
        }
    }

    impl From<&str> for ManagerInfo {
        fn from(value: &str) -> Self {
            Self {
                manager: ManagerType::Slurm,
                allocation_id: value.to_string(),
                time_limit: None,
                max_memory_mb: None,
            }
        }
    }

    impl From<QueueBuilder> for AllocationQueue {
        fn from(builder: QueueBuilder) -> Self {
            let (params, limiter) = builder.build();
            Self::new(
                QueueInfo::new(params),
                None,
                always_queued_handler(),
                limiter,
                None,
            )
        }
    }

    fn add_alloc(queue: &mut AllocationQueue, id: u64, worker_count: u64) {
        queue.add_allocation(Allocation::new(
            id.to_string(),
            worker_count,
            PathBuf::new().into(),
        ));
    }

    fn query_response_sn(single_node_workers: u32) -> QueryResponse {
        QueryResponse {
            single_node_workers,
            multinode_allocations: 0,
            multinode_workers_per_alloc: 0,
        }
    }

    struct Handler<ScheduleFn, StatusFn, RemoveFn, State> {
        schedule_fn: WrappedRcRefCell<ScheduleFn>,
        status_fn: WrappedRcRefCell<StatusFn>,
        remove_fn: WrappedRcRefCell<RemoveFn>,
        custom_state: WrappedRcRefCell<State>,
    }

    impl<
        State: 'static,
        ScheduleFn: 'static + Fn(WrappedRcRefCell<State>, u64) -> ScheduleFnFut,
        ScheduleFnFut: Future<Output = AutoAllocResult<AllocationSubmissionResult>>,
        StatusFn: 'static + Fn(WrappedRcRefCell<State>, AllocationId) -> StatusFnFut,
        StatusFnFut: Future<Output = AutoAllocResult<Option<AllocationExternalStatus>>>,
        RemoveFn: 'static + Fn(WrappedRcRefCell<State>, AllocationId) -> RemoveFnFut,
        RemoveFnFut: Future<Output = AutoAllocResult<()>>,
    > Handler<ScheduleFn, StatusFn, RemoveFn, State>
    {
        fn create(
            custom_state: WrappedRcRefCell<State>,
            schedule_fn: ScheduleFn,
            status_fn: StatusFn,
            remove_fn: RemoveFn,
        ) -> Box<dyn QueueHandler> {
            Box::new(Self {
                schedule_fn: WrappedRcRefCell::wrap(schedule_fn),
                status_fn: WrappedRcRefCell::wrap(status_fn),
                remove_fn: WrappedRcRefCell::wrap(remove_fn),
                custom_state,
            })
        }
    }

    impl<
        State: 'static,
        ScheduleFn: 'static + Fn(WrappedRcRefCell<State>, u64) -> ScheduleFnFut,
        ScheduleFnFut: Future<Output = AutoAllocResult<AllocationSubmissionResult>>,
        StatusFn: 'static + Fn(WrappedRcRefCell<State>, AllocationId) -> StatusFnFut,
        StatusFnFut: Future<Output = AutoAllocResult<Option<AllocationExternalStatus>>>,
        RemoveFn: 'static + Fn(WrappedRcRefCell<State>, AllocationId) -> RemoveFnFut,
        RemoveFnFut: Future<Output = AutoAllocResult<()>>,
    > QueueHandler for Handler<ScheduleFn, StatusFn, RemoveFn, State>
    {
        fn submit_allocation(
            &mut self,
            _queue_id: QueueId,
            _queue_info: &QueueInfo,
            worker_count: u64,
            _mode: SubmitMode,
        ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationSubmissionResult>>>> {
            let schedule_fn = self.schedule_fn.clone();
            let custom_state = self.custom_state.clone();

            Box::pin(async move { (schedule_fn.get())(custom_state.clone(), worker_count).await })
        }

        fn get_status_of_allocations(
            &self,
            allocations: &[&Allocation],
        ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationStatusMap>>>> {
            let status_fn = self.status_fn.clone();
            let custom_state = self.custom_state.clone();
            let allocation_ids: Vec<AllocationId> =
                allocations.iter().map(|alloc| alloc.id.clone()).collect();

            Box::pin(async move {
                let mut result = Map::default();
                for allocation_id in allocation_ids {
                    let status =
                        (status_fn.get())(custom_state.clone(), allocation_id.clone()).await;
                    if let Some(status) = status.transpose() {
                        result.insert(allocation_id, status);
                    }
                }
                Ok(result)
            })
        }

        fn remove_allocation(
            &self,
            allocation: &Allocation,
        ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>> {
            let remove_fn = self.remove_fn.clone();
            let custom_state = self.custom_state.clone();
            let allocation_id = allocation.id.clone();

            Box::pin(async move { (remove_fn.get())(custom_state.clone(), allocation_id).await })
        }
    }

    #[derive(Default)]
    struct HandlerState {
        state: Map<AllocationId, AllocationExternalStatus>,
        spawned_allocations: Set<AllocationId>,
        allocation_attempts: usize,
        allocation_will_fail: bool,
        deleted_allocations: Set<AllocationId>,
    }

    // Handlers
    fn always_queued_handler() -> Box<dyn QueueHandler> {
        Handler::create(
            WrappedRcRefCell::wrap(0),
            move |state, _| async move {
                let mut s = state.get_mut();
                let id = *s;
                *s += 1;
                Ok(AllocationSubmissionResult::new(
                    Ok(id.to_string()),
                    PathBuf::default().into(),
                ))
            },
            move |_, _| async move { Ok(Some(AllocationExternalStatus::Queued)) },
            |_, _| async move { Ok(()) },
        )
    }

    fn fails_submit_handler() -> Box<dyn QueueHandler> {
        Handler::create(
            WrappedRcRefCell::wrap(()),
            move |_, _| async move {
                let tempdir = TempDir::with_prefix("hq").unwrap();
                let dir = tempdir.keep();

                Ok(AllocationSubmissionResult::new(
                    Err(anyhow::anyhow!("failure")),
                    dir.into(),
                ))
            },
            move |_, _| async move { Ok(Some(AllocationExternalStatus::Queued)) },
            |_, _| async move { Ok(()) },
        )
    }

    /// Handler that spawns allocations with sequentially increasing IDs (starting at *0*) and
    /// returns allocation statuses from [`HandlerState`].
    fn stateful_handler(state: WrappedRcRefCell<HandlerState>) -> Box<dyn QueueHandler> {
        Handler::create(
            state,
            move |state, _worker_count| async move {
                let mut state = state.get_mut();
                let tempdir = TempDir::with_prefix("hq").unwrap();
                let dir = tempdir.keep();

                state.allocation_attempts += 1;

                if state.allocation_will_fail {
                    Ok(AllocationSubmissionResult::new(
                        AutoAllocResult::Err(anyhow!("Fail")),
                        dir.into(),
                    ))
                } else {
                    let spawned = &mut state.spawned_allocations;
                    let id = spawned.len();
                    spawned.insert(id.to_string());

                    Ok(AllocationSubmissionResult::new(
                        Ok(id.to_string()),
                        dir.into(),
                    ))
                }
            },
            move |state, id| async move {
                let state = &mut state.get_mut().state;
                let status = state
                    .get(&id)
                    .cloned()
                    .unwrap_or(AllocationExternalStatus::Queued);
                Ok(Some(status))
            },
            |state, id| async move {
                state.get_mut().deleted_allocations.insert(id);
                Ok(())
            },
        )
    }

    // Queue definitions
    #[derive(Builder)]
    #[builder(pattern = "owned", build_fn(name = "finish"))]
    struct Queue {
        #[builder(default = "ManagerType::Slurm")]
        manager: ManagerType,
        #[builder(default = "1")]
        backlog: u32,
        #[builder(default = "1")]
        max_workers_per_alloc: u32,
        #[builder(default = "Duration::from_secs(60 * 60)")]
        timelimit: Duration,
        #[builder(default)]
        max_worker_count: Option<u32>,
        #[builder(default = "100")]
        limiter_max_alloc_fails: u64,
        #[builder(default = "100")]
        limiter_max_submit_fails: u64,
        #[builder(default = "vec![Duration::ZERO]")]
        limiter_delays: Vec<Duration>,
        #[builder(default)]
        min_utilization: Option<f32>,
        #[builder(default)]
        cli_resources: Option<ResourceDescriptor>,
    }

    impl QueueBuilder {
        fn build(self) -> (QueueParameters, RateLimiter) {
            let Queue {
                manager,
                backlog,
                max_workers_per_alloc,
                timelimit,
                max_worker_count,
                limiter_max_alloc_fails,
                limiter_max_submit_fails,
                limiter_delays,
                min_utilization,
                cli_resources,
            } = self.finish().unwrap();
            let params = QueueParameters {
                manager,
                max_workers_per_alloc,
                backlog,
                timelimit,
                name: None,
                max_worker_count,
                min_utilization,
                additional_args: vec![],
                worker_start_cmd: None,
                worker_stop_cmd: None,
                worker_wrap_cmd: None,
                cli_resource_descriptor: cli_resources,
                worker_args: vec![],
                idle_timeout: None,
            };

            (
                params,
                RateLimiter::new(
                    limiter_delays,
                    limiter_max_submit_fails,
                    limiter_max_alloc_fails,
                ),
            )
        }
    }

    fn lost_worker_normal(reason: LostWorkerReason) -> LostWorkerDetails {
        LostWorkerDetails {
            reason,
            lifetime: Duration::from_secs(3600),
        }
    }
    fn lost_worker_quick(reason: LostWorkerReason) -> LostWorkerDetails {
        LostWorkerDetails {
            reason,
            lifetime: Duration::from_millis(1),
        }
    }
}
