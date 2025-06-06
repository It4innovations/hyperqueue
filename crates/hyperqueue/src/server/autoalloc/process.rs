use std::future::Future;
use std::path::PathBuf;
use std::time::{Instant, SystemTime};

use crate::common::manager::info::{ManagerInfo, ManagerType};
use crate::common::rpc::RpcReceiver;
use crate::get_or_return;
use crate::server::autoalloc::config::{
    get_allocation_refresh_interval, get_allocation_schedule_tick_interval, get_max_allocation_schedule_delay,
    max_allocation_fails, MAX_QUEUED_STATUS_ERROR_COUNT, MAX_RUNNING_STATUS_ERROR_COUNT,
    MAX_SUBMISSION_FAILS, SUBMISSION_DELAYS,
};
use crate::server::autoalloc::queue::pbs::PbsHandler;
use crate::server::autoalloc::queue::slurm::SlurmHandler;
use crate::server::autoalloc::queue::{AllocationExternalStatus, QueueHandler, SubmitMode};
use crate::server::autoalloc::service::{AutoAllocMessage, LostWorkerDetails};
use crate::server::autoalloc::state::{
    AllocationQueue, AllocationQueueState, AllocationState, AutoAllocState, RateLimiter,
    RateLimiterStatus,
};
use crate::server::autoalloc::{Allocation, AllocationId, AutoAllocResult, QueueId, QueueInfo};
use crate::server::event::streamer::EventStreamer;
use crate::transfer::messages::{AllocationQueueParams, QueueData, QueueState};
use futures::future::join_all;
use tako::control::{ServerRef, WorkerTypeQuery};
use tako::resources::{
    ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind, CPU_RESOURCE_NAME,
};
use tako::WorkerId;
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

    // When no message is not received after this interval, run scheduling
    let max_scheduling_delay = get_max_allocation_schedule_delay();
    // How often to check scheduling
    let mut scheduling_interval = tokio::time::interval(get_allocation_schedule_tick_interval());

    // Should scheduling be performed at the next scheduling "tick"?
    let mut should_schedule = false;
    let mut last_schedule = Instant::now();

    loop {
        tokio::select! {
            _ = periodic_update_interval.tick() => {
                do_periodic_update(&senders, &mut autoalloc).await;
            }
            _ = scheduling_interval.tick() => {
                if should_schedule || last_schedule.elapsed() >= max_scheduling_delay {
                    if let Err(error) = perform_submits(&mut autoalloc, &senders).await {
                        log::error!("Autoalloc scheduling failed: {error:?}");
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
pub async fn try_submit_allocation(params: AllocationQueueParams) -> anyhow::Result<()> {
    let tmpdir = TempDir::with_prefix("hq")?;
    let mut handler = create_allocation_handler(
        &params.manager,
        params.name.clone(),
        tmpdir.as_ref().to_path_buf(),
    )?;
    let worker_count = params.workers_per_alloc;
    let queue_info = create_queue_info(params);

    let allocation = handler
        .submit_allocation(0, &queue_info, worker_count as u64, SubmitMode::DryRun)
        .await
        .map_err(|e| anyhow::anyhow!("Could not submit allocation: {:?}", e))?;

    let working_dir = allocation.working_dir().to_path_buf();
    let id = allocation
        .into_id()
        .map_err(|e| anyhow::anyhow!("Could not submit allocation: {:?}", e))?;
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

pub fn create_queue_info(params: AllocationQueueParams) -> QueueInfo {
    let AllocationQueueParams {
        manager,
        name: _name,
        workers_per_alloc,
        backlog,
        timelimit,
        additional_args,
        max_worker_count,
        worker_start_cmd,
        worker_stop_cmd,
        worker_args,
        idle_timeout,
    } = params;
    QueueInfo::new(
        manager,
        backlog,
        workers_per_alloc,
        timelimit,
        additional_args,
        max_worker_count,
        worker_args,
        idle_timeout,
        worker_start_cmd,
        worker_stop_cmd,
    )
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
        AutoAllocMessage::WorkerConnected(id, manager_info) => {
            log::debug!(
                "Registering worker {id} for allocation {}",
                manager_info.allocation_id
            );
            if let Some((queue, queue_id, allocation_id)) =
                get_data_from_worker(autoalloc, &manager_info)
            {
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
                            info: queue.info().clone(),
                            name: queue.name().map(|name| name.to_string()),
                            manager_type: queue.manager().clone(),
                            state: match queue.state() {
                                AllocationQueueState::Active => QueueState::Active,
                                AllocationQueueState::Paused => QueueState::Paused,
                            },
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
            response,
        } => {
            log::debug!("Creating queue, params={params:?}");
            let result = create_queue(autoalloc, events, server_directory, params, queue_id);
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
        AutoAllocMessage::GetAllocations(queue_id, response) => {
            let result = match autoalloc.get_queue(queue_id) {
                Some(queue) => Ok(queue.all_allocations().cloned().collect()),
                None => Err(anyhow::anyhow!("Queue {queue_id} not found")),
            };
            response.respond(result);
            false
        }
        AutoAllocMessage::QuitService => unreachable!(),
    }
}

/// Perform an allocation submission pass.
/// Ask the scheduler how many workers it wants to be created, and spawn them into the individual
/// allocation queues.
async fn perform_submits(
    autoalloc: &mut AutoAllocState,
    senders: &AutoallocSenders,
) -> anyhow::Result<()> {
    // Figure out which queues are active
    let queues = autoalloc
        .queues()
        .filter(|(_, queue)| queue.state().is_active())
        .collect::<Vec<_>>();
    if queues.is_empty() {
        return Ok(());
    }

    // Ask the scheduler how many workers it needs
    let queries: Vec<WorkerTypeQuery> = queues
        .iter()
        .map(|(_, queue)| create_queue_worker_query(queue))
        .collect();
    let response = senders.server.new_worker_query(queries)?;

    // Now schedule the workers into the individual queues
    let queue_ids: Vec<QueueId> = queues.into_iter().map(|(id, _)| id).collect();
    for (required_workers, queue_id) in response.single_node_workers_per_query.iter().zip(queue_ids)
    {
        queue_try_submit(autoalloc, queue_id, senders, *required_workers as u32).await;
        try_pause_queue(autoalloc, queue_id);
    }

    Ok(())
}

/// Create a worker query that corresponds to the provided resources of the given `queue`.
fn create_queue_worker_query(queue: &AllocationQueue) -> WorkerTypeQuery {
    let info = queue.info();

    WorkerTypeQuery {
        // The maximum number of workers that we can provide in this queue
        // TODO: estimate the resources of the queue in a better way
        descriptor: ResourceDescriptor::new(vec![ResourceDescriptorItem {
            name: CPU_RESOURCE_NAME.to_string(),
            kind: ResourceDescriptorKind::regular_sockets(1, 1),
        }]),
        time_limit: Some(info.timelimit()),
        // How many workers can we provide at the moment
        max_sn_workers: info.backlog() * info.workers_per_alloc(),
        max_workers_per_allocation: info.workers_per_alloc(),
        // TODO: expose this through the CLI
        min_utilization: 0.0,
    }
}

/// Permits the autoallocator to submit a given number of allocations with the given number of
/// workers, based on queue limits.
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
fn compute_submission_permit(queue: &AllocationQueue, required_workers: u32) -> SubmissionPermit {
    let info = queue.info();

    // How many workers are currently already queued or running?
    let active_workers = queue
        .active_allocations()
        .map(|allocation| allocation.target_worker_count as u32)
        .sum::<u32>();
    let queued_allocs = queue.queued_allocations().count() as u32;

    let mut max_workers_to_spawn = match info.max_worker_count() {
        Some(max) => max.saturating_sub(active_workers),
        None => u32::MAX,
    };

    log::debug!(
        r"Counting possible allocations: required worker count is {required_workers}.
Backlog: {}, currently queued: {queued_allocs}, max workers to spawn: {max_workers_to_spawn}
",
        info.backlog(),
    );

    if max_workers_to_spawn == 0 {
        return SubmissionPermit::empty();
    }

    // Start with backlog
    let allocs_to_submit = info.backlog();
    // Subtract already queued allocations
    let allocs_to_submit = allocs_to_submit.saturating_sub(queued_allocs);
    // Only create enough allocations to serve `required_workers`.
    // Allocations that are already queued will be subtracted from this.
    let missing_workers = required_workers.saturating_sub(queued_allocs * info.workers_per_alloc());
    let missing_allocations =
        ((missing_workers as f32) / (info.workers_per_alloc() as f32)).ceil() as u32;
    let allocs_to_submit = allocs_to_submit.min(missing_allocations);

    let mut allocations = vec![];
    for _ in 0..allocs_to_submit {
        let remaining_workers = max_workers_to_spawn.saturating_sub(info.workers_per_alloc());
        if remaining_workers == 0 {
            break;
        }
        let to_spawn = info.workers_per_alloc().min(remaining_workers);
        max_workers_to_spawn = max_workers_to_spawn.saturating_sub(to_spawn);
        allocations.push(to_spawn as u64);
    }

    log::debug!("Determined allocations to submit: {allocs_to_submit:?}");

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
    required_workers: u32,
) {
    if required_workers == 0 {
        return;
    }

    // Figure out how many allocations we are allowed to submit, according to queue limits
    // Also check the rate limiter
    let permit = {
        let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));
        if !queue.state().is_active() {
            return;
        }

        let permit = compute_submission_permit(queue, required_workers);
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
                let working_dir = submission_result.working_dir().to_path_buf();
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
    params: AllocationQueueParams,
    queue_id: Option<QueueId>,
) -> anyhow::Result<QueueId> {
    let name = params.name.clone();
    let handler = create_allocation_handler(&params.manager, name.clone(), server_directory);
    let queue_info = create_queue_info(params.clone());

    match handler {
        Ok(handler) => {
            let queue = AllocationQueue::new(queue_info, name, handler, create_rate_limiter());
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
            Ok(_) => log::info!("Allocation {} was removed", allocation_id),
            Err(e) => log::error!("Failed to remove allocation {}: {:?}", allocation_id, e),
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
                        finished_at: SystemTime::now(),
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
                    finished_at: SystemTime::now(),
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
                    finished_at: SystemTime::now(),
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

    enum AllocationFinished {
        Failure,
        Success,
    }

    let finished: Option<AllocationFinished> = {
        match sync_reason {
            AllocationSyncReason::WorkedConnected(worker_id) => match &mut allocation.status {
                AllocationState::Queued { .. } => {
                    allocation.status = AllocationState::Running {
                        connected_workers: Set::from_iter([worker_id]),
                        disconnected_workers: Default::default(),
                        started_at: SystemTime::now(),
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
                                finished_at: SystemTime::now(),
                                disconnected_workers: std::mem::take(disconnected_workers),
                            };

                            if is_failed {
                                Some(AllocationFinished::Failure)
                            } else {
                                Some(AllocationFinished::Success)
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
                        // The allocation has started running. Usually, we will receive
                        // information about this by a worker being connected, but in theory this
                        // event can happen sooner (or the allocation can start, but the worker
                        // fails to start).
                        allocation.status = AllocationState::Running {
                            started_at: SystemTime::now(),
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
                            AllocationFinished::Failure
                        } else {
                            AllocationFinished::Success
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
                            AllocationFinished::Failure
                        } else {
                            AllocationFinished::Success
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
        Some(AllocationFinished::Success) => {
            log::debug!("Marking allocation {allocation_id} success");
            queue.limiter_mut().on_allocation_success();
            events.on_allocation_finished(queue_id, allocation_id.to_string());
        }
        Some(AllocationFinished::Failure) => {
            log::debug!("Marking allocation {allocation_id} failure");
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

fn try_pause_queue(autoalloc: &mut AutoAllocState, id: QueueId) {
    let queue = get_or_return!(autoalloc.get_queue_mut(id));
    if !queue.state().is_active() {
        return;
    }
    let limiter = queue.limiter();

    let status = limiter.submission_status();
    match status {
        RateLimiterStatus::TooManyFailedSubmissions => {
            log::error!("The queue {id} had too many failed submissions, it will be paused.");
            queue.pause();
        }
        RateLimiterStatus::TooManyFailedAllocations => {
            log::error!("The queue {id} had too many failed allocations, it will be paused.");
            queue.pause();
        }
        RateLimiterStatus::Ok | RateLimiterStatus::Wait => (),
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

    use std::time::Duration;

    use crate::common::arraydef::IntArray;
    use crate::common::manager::info::{ManagerInfo, ManagerType, WORKER_EXTRA_MANAGER_KEY};
    use crate::common::rpc::ResponseToken;
    use crate::server::autoalloc::process::{
        handle_message, perform_submits, sync_allocation_status,
        AllocationSyncReason, AutoallocSenders,
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
    };
    use crate::server::event::streamer::EventStreamer;
    use crate::server::job::{Job, SubmittedJobDescription};
    use crate::server::state::{State, StateRef};
    use crate::tests::utils::create_hq_state;
    use crate::transfer::messages::{
        AllocationQueueParams, JobDescription, JobSubmitDescription, JobTaskDescription, PinMode,
        TaskDescription, TaskKind, TaskKindProgram,
    };
    use anyhow::anyhow;
    use chrono::Utc;
    use derive_builder::Builder;
    use log::LevelFilter;
    use smallvec::smallvec;
    use tako::gateway::{
        CrashLimit, LostWorkerReason, ResourceRequest, ResourceRequestEntry,
        ResourceRequestVariants,
    };
    use tako::program::ProgramDefinition;
    use tako::resources::ResourceAmount;
    use tako::resources::{AllocationRequest, TimeRequest, CPU_RESOURCE_NAME};
    use tako::tests::integration::utils::server::{
        run_server_test, ServerConfigBuilder, ServerHandle,
    };
    use tako::tests::integration::utils::task::{
        simple_args, GraphBuilder, TaskConfigBuilder,
    };
    use tako::tests::integration::utils::worker::WorkerConfigBuilder;
    use tako::WorkerId;
    use tako::{Map, Set, WrappedRcRefCell};
    use tempfile::TempDir;

    struct TestCtx {
        state: AutoAllocState,
        senders: AutoallocSenders,
        handle: ServerHandle,
    }

    impl TestCtx {
        async fn add_queue(
            &mut self,
            handler: Box<dyn QueueHandler>,
            queue_builder: QueueBuilder,
        ) -> QueueId {
            let (queue_info, _rate_limiter) = queue_builder.build();
            let (token, rx) = ResponseToken::new();
            handle_message(
                &mut self.state,
                &self.senders.events,
                AutoAllocMessage::AddQueue {
                    server_directory: PathBuf::from("test"),
                    params: AllocationQueueParams {
                        manager: queue_info.manager().clone(),
                        workers_per_alloc: queue_info.workers_per_alloc(),
                        backlog: queue_info.backlog(),
                        timelimit: queue_info.timelimit(),
                        name: Some("Queue".to_string()),
                        max_worker_count: queue_info.max_worker_count(),
                        additional_args: vec![],
                        worker_start_cmd: None,
                        worker_stop_cmd: None,
                        worker_args: queue_info.worker_args().iter().cloned().collect(),
                        idle_timeout: None,
                    },
                    queue_id: None,
                    response: token,
                },
            )
            .await;
            let queue_id = rx.await.unwrap().unwrap();

            // A bit hacky, but easier than creating a mock for an external service, as we do in
            // Python.
            self.state
                .get_queue_mut(queue_id)
                .unwrap()
                .set_handler(handler);

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

        async fn start_worker<Id: Into<WorkerId>>(&mut self, worker_id: Id, info: ManagerInfo) {
            assert!(
                handle_message(
                    &mut self.state,
                    &self.senders.events,
                    AutoAllocMessage::WorkerConnected(worker_id.into(), info)
                )
                .await
            );
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
    }

    async fn run_test<F: AsyncFnOnce(TestCtx)>(f: F) {
        let _ = env_logger::Builder::default()
            .filter(None, LevelFilter::Debug)
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

    #[tokio::test]
    async fn no_submit_without_tasks() {
        run_test(async |mut ctx: TestCtx| {
            let handler = always_queued_handler();
            let queue_id = ctx
                .add_queue(
                    handler,
                    QueueBuilder::default().backlog(4).workers_per_alloc(2),
                )
                .await;
            ctx.try_submit().await;
            assert!(ctx.get_allocations(queue_id).is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn fill_backlog() {
        run_test(async |mut ctx: TestCtx| {
            let handler = always_queued_handler();
            let queue_id = ctx
                .add_queue(
                    handler,
                    QueueBuilder::default().backlog(4).workers_per_alloc(2),
                )
                .await;

            ctx.create_simple_tasks(100).await;
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

    #[tokio::test]
    async fn do_nothing_on_full_backlog() {
        run_test(async |mut ctx: TestCtx| {
            let handler = always_queued_handler();
            let queue_id = ctx
                .add_queue(
                    handler,
                    QueueBuilder::default().backlog(4).workers_per_alloc(1),
                )
                .await;

            ctx.create_simple_tasks(100).await;

            for _ in 0..5 {
                ctx.try_submit().await;
            }

            let allocations = ctx.get_allocations(queue_id);
            assert_eq!(allocations.len(), 4);
        })
        .await;
    }

    #[tokio::test]
    async fn worker_connects_from_unknown_allocation() {
        run_test(async |mut ctx: TestCtx| {
            let handler = always_queued_handler();
            let queue_id = ctx
                .add_queue(
                    handler,
                    QueueBuilder::default().backlog(1).workers_per_alloc(1),
                )
                .await;

            ctx.create_simple_tasks(100).await;
            ctx.try_submit().await;
            ctx.start_worker(
                0,
                ManagerInfo::new(
                    ManagerType::Pbs,
                    // Unknown allocation
                    "foo".to_string(),
                    None,
                ),
            )
            .await;

            assert!(
                ctx.get_allocations(queue_id)
                    .iter()
                    .all(|alloc| !alloc.is_running())
            );
        })
        .await;
    }

    /*#[tokio::test]
    async fn start_allocation_when_worker_connects() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(1),
        );

        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        let allocs = get_allocations(&state, queue_id);

        on_worker_added(&s, queue_id, &mut state, &allocs[0].id, 10);
        check_running_workers(get_allocations(&state, queue_id).first().unwrap(), vec![10]);
    }

    #[tokio::test]
    async fn add_another_worker_to_allocation() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(2),
        );

        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        let allocs = get_allocations(&state, queue_id);

        for id in [0u32, 1] {
            on_worker_added(&s, queue_id, &mut state, &allocs[0].id, id);
        }
        check_running_workers(
            get_allocations(&state, queue_id).first().unwrap(),
            vec![0, 1],
        );
    }

    #[tokio::test]
    async fn finish_allocation_when_worker_disconnects() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(1),
        );

        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        let allocs = get_allocations(&state, queue_id);

        on_worker_added(&s, queue_id, &mut state, &allocs[0].id, 0);
        on_worker_lost(
            &s,
            queue_id,
            &mut state,
            &allocs[0].id,
            0,
            lost_worker_normal(LostWorkerReason::ConnectionLost),
        );
        check_finished_workers(
            get_allocations(&state, queue_id).first().unwrap(),
            vec![(0, LostWorkerReason::ConnectionLost)],
        );
    }

    #[tokio::test]
    async fn finish_allocation_when_last_worker_disconnects() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(2),
        );

        let s = EventStreamer::new(None);

        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        let allocs = get_allocations(&state, queue_id);

        for id in [0u32, 1] {
            on_worker_added(&s, queue_id, &mut state, &allocs[0].id, id);
        }
        on_worker_lost(
            &s,
            queue_id,
            &mut state,
            &allocs[0].id,
            0,
            lost_worker_normal(LostWorkerReason::ConnectionLost),
        );
        let allocation = get_allocations(&state, queue_id)[0].clone();
        check_running_workers(&allocation, vec![1]);
        check_disconnected_running_workers(
            &allocation,
            vec![(0, LostWorkerReason::ConnectionLost)],
        );
        on_worker_lost(
            &s,
            queue_id,
            &mut state,
            &allocs[0].id,
            1,
            lost_worker_normal(LostWorkerReason::HeartbeatLost),
        );
        let allocation = get_allocations(&state, queue_id)[0].clone();
        check_finished_workers(
            &allocation,
            vec![
                (0, LostWorkerReason::ConnectionLost),
                (1, LostWorkerReason::HeartbeatLost),
            ],
        );
    }

    #[tokio::test]
    async fn do_not_create_allocations_without_tasks() {
        let hq_state = new_hq_state(0);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(&mut state, handler, QueueBuilder::default().backlog(3));

        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 0);
    }

    #[tokio::test]
    async fn do_not_fill_backlog_when_tasks_run_out() {
        let hq_state = new_hq_state(5);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(5).workers_per_alloc(2),
        );

        // 5 tasks, 3 * 2 workers -> last two allocations should be ignored
        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 3);
    }

    #[tokio::test]
    async fn stop_allocating_on_error() {
        let hq_state = new_hq_state(5);
        let mut state = AutoAllocState::new(1);

        let handler_state = WrappedRcRefCell::wrap(HandlerState::default());
        let handler = stateful_handler(handler_state.clone());
        let queue_id = add_queue(&mut state, handler, QueueBuilder::default().backlog(5));

        handler_state.get_mut().allocation_will_fail = true;

        // Only try the first allocation in the backlog
        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        assert_eq!(handler_state.get().allocation_attempts, 1);

        handler_state.get_mut().allocation_will_fail = false;

        // Finish the rest
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        assert_eq!(handler_state.get().allocation_attempts, 6);
    }

    #[tokio::test]
    async fn ignore_task_with_high_time_request() {
        let hq_state = new_hq_state(0);
        attach_job(&mut hq_state.get_mut(), 0, 1, Duration::from_secs(60 * 60));
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().timelimit(Duration::from_secs(60 * 30)),
        );

        // Allocations last for 30 minutes, but job requires 60 minutes
        // Nothing should be scheduled
        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 0);
    }

    #[tokio::test]
    async fn respect_max_worker_count() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new(1);

        let handler_state = WrappedRcRefCell::wrap(HandlerState::default());
        let handler = stateful_handler(handler_state.clone());

        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(4).max_worker_count(Some(5)),
        );

        let s = EventStreamer::new(None);
        // Put 4 allocations into the queue.
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        let allocations = get_allocations(&state, queue_id);
        assert_eq!(allocations.len(), 4);

        // Start 2 allocations
        on_worker_added(&s, queue_id, &mut state, &allocations[0].id, 0);
        on_worker_added(&s, queue_id, &mut state, &allocations[1].id, 1);

        // Create only one additional allocation
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 5);

        // Finish one allocation
        on_worker_lost(
            &s,
            queue_id,
            &mut state,
            &allocations[1].id,
            1,
            lost_worker_normal(LostWorkerReason::ConnectionLost),
        );

        // One worker was freed, create an additional allocation
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 6);
    }

    #[tokio::test]
    async fn max_worker_count_shorten_last_allocation() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();

        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default()
                .backlog(2)
                .workers_per_alloc(4)
                .max_worker_count(Some(6)),
        );

        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        let allocations = get_allocations(&state, queue_id);
        assert_eq!(allocations.len(), 2);
        assert_eq!(allocations[0].target_worker_count, 4);
        assert_eq!(allocations[1].target_worker_count, 2);
    }

    #[tokio::test]
    async fn delete_stale_directories_of_unsubmitted_allocations() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new(1);

        let make_dir = || {
            let tempdir = TempDir::with_prefix("hq").unwrap();
            tempdir.into_path()
        };

        let handler = fails_submit_handler();

        let max_kept = 2;
        state.set_max_kept_directories(max_kept);
        add_queue(&mut state, handler, QueueBuilder::default());

        let dirs = [make_dir(), make_dir()];
        state.set_inactive_allocation_directories(dirs.iter().cloned().collect());

        let s = EventStreamer::new(None);
        // Delete oldest directory
        do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
        assert!(!dirs[0].exists());
        assert!(dirs[1].exists());

        // Delete second oldest directory
        do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
        assert!(!dirs[1].exists());
    }

    #[tokio::test]
    async fn pause_queue_when_submission_fails_too_many_times() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new(1);

        let shared = WrappedRcRefCell::wrap(HandlerState::default());
        let handler = stateful_handler(shared.clone());

        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default()
                .backlog(5)
                .limiter_max_submit_fails(2),
        );

        shared.get_mut().allocation_will_fail = true;

        let s = EventStreamer::new(None);
        do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
        check_queue_exists(&state, queue_id);
        do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
        check_queue_paused(&state, queue_id);
    }

    #[tokio::test]
    async fn pause_queue_when_allocation_fails_too_many_times() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new(1);

        let shared = WrappedRcRefCell::wrap(HandlerState::default());
        let handler = stateful_handler(shared.clone());

        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default()
                .backlog(5)
                .limiter_max_alloc_fails(2),
        );
        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;

        let allocations = get_allocations(&state, queue_id);
        fail_allocation(queue_id, &mut state, &allocations[0].id);
        do_periodic_update(
            &hq_state,
            &s,
            &mut state,
            RefreshReason::UpdateQueue(queue_id),
        )
        .await;
        check_queue_exists(&state, queue_id);

        fail_allocation(queue_id, &mut state, &allocations[1].id);
        do_periodic_update(
            &hq_state,
            &s,
            &mut state,
            RefreshReason::UpdateQueue(queue_id),
        )
        .await;
        check_queue_paused(&state, queue_id);
    }

    #[tokio::test]
    async fn respect_rate_limiter() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new(1);

        let shared = WrappedRcRefCell::wrap(HandlerState::default());
        let handler = stateful_handler(shared.clone());

        add_queue(
            &mut state,
            handler,
            QueueBuilder::default()
                .backlog(5)
                .limiter_max_alloc_fails(100)
                .limiter_delays(vec![
                    Duration::ZERO,
                    Duration::from_secs(1),
                    Duration::from_secs(10),
                ]),
        );

        shared.get_mut().allocation_will_fail = true;

        let check_alloc_count = |count: usize| {
            assert_eq!(shared.get().allocation_attempts, count);
        };
        let s = EventStreamer::new(None);
        let mut now = Instant::now();
        {
            let _mock = MockTime::mock(now);
            do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
            check_alloc_count(1);
        }

        // Now we should sleep until +1 sec
        {
            now += Duration::from_millis(500);
            let _mock = MockTime::mock(now);
            do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
            check_alloc_count(1);
        }
        {
            now += Duration::from_millis(1500);
            let _mock = MockTime::mock(now);
            do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
            check_alloc_count(2);
        }

        // Now we should sleep until +10 sec
        {
            now += Duration::from_millis(5000);
            let _mock = MockTime::mock(now);
            do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
            check_alloc_count(2);
        }
        {
            now += Duration::from_millis(6000);
            let _mock = MockTime::mock(now);
            do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
            check_alloc_count(3);
        }
        // The delay shouldn't increase any more when we have reached the maximum delay
        {
            now += Duration::from_millis(11000);
            let _mock = MockTime::mock(now);
            do_periodic_update(&hq_state, &s, &mut state, RefreshReason::UpdateAllQueues).await;
            check_alloc_count(4);
        }
    }

    #[tokio::test]
    async fn bump_fail_counter_if_worker_fails_quick() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(1),
        );

        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        let allocs = get_allocations(&state, queue_id);

        fail_allocation(queue_id, &mut state, &allocs[0].id);
        assert_eq!(
            state
                .get_queue(queue_id)
                .unwrap()
                .limiter()
                .allocation_fail_count(),
            1
        );
    }

    #[tokio::test]
    async fn do_not_bump_fail_counter_if_worker_fails_normally() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new(1);

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(1),
        );

        let s = EventStreamer::new(None);
        queue_try_submit(queue_id, &mut state, &hq_state, &s, None).await;
        let allocs = get_allocations(&state, queue_id);

        on_worker_added(&s, queue_id, &mut state, &allocs[0].id, 0);
        on_worker_lost(
            &s,
            queue_id,
            &mut state,
            &allocs[0].id,
            0,
            lost_worker_normal(LostWorkerReason::ConnectionLost),
        );
        assert_eq!(
            state
                .get_queue(queue_id)
                .unwrap()
                .limiter()
                .allocation_fail_count(),
            0
        );
    }*/

    // Utilities
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
        fn new(
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
        Handler::new(
            WrappedRcRefCell::wrap(0),
            move |state, _| async move {
                let mut s = state.get_mut();
                let id = *s;
                *s += 1;
                Ok(AllocationSubmissionResult::new(
                    Ok(id.to_string()),
                    Default::default(),
                ))
            },
            move |_, _| async move { Ok(Some(AllocationExternalStatus::Queued)) },
            |_, _| async move { Ok(()) },
        )
    }

    fn fails_submit_handler() -> Box<dyn QueueHandler> {
        Handler::new(
            WrappedRcRefCell::wrap(()),
            move |_, _| async move {
                let tempdir = TempDir::with_prefix("hq").unwrap();
                let dir = tempdir.into_path();

                Ok(AllocationSubmissionResult::new(
                    Err(anyhow::anyhow!("failure")),
                    dir,
                ))
            },
            move |_, _| async move { Ok(Some(AllocationExternalStatus::Queued)) },
            |_, _| async move { Ok(()) },
        )
    }

    fn fails_status_handler() -> Box<dyn QueueHandler> {
        Handler::new(
            WrappedRcRefCell::wrap(0),
            move |state, _| async move {
                let mut s = state.get_mut();
                let id = *s;
                *s += 1;
                Ok(AllocationSubmissionResult::new(
                    Ok(id.to_string()),
                    Default::default(),
                ))
            },
            move |_, alloc_id| async move {
                Err(anyhow::anyhow!(
                    "Cannot get status of allocation {alloc_id}"
                ))
            },
            |_, _| async move { Ok(()) },
        )
    }

    /// Handler that spawns allocations with sequentially increasing IDs (starting at *0*) and
    /// returns allocation statuses from [`HandlerState`].
    fn stateful_handler(state: WrappedRcRefCell<HandlerState>) -> Box<dyn QueueHandler> {
        Handler::new(
            state,
            move |state, _worker_count| async move {
                let mut state = state.get_mut();
                let tempdir = TempDir::with_prefix("hq").unwrap();
                let dir = tempdir.into_path();

                state.allocation_attempts += 1;

                if state.allocation_will_fail {
                    Ok(AllocationSubmissionResult::new(
                        AutoAllocResult::Err(anyhow!("Fail")),
                        dir,
                    ))
                } else {
                    let spawned = &mut state.spawned_allocations;
                    let id = spawned.len();
                    spawned.insert(id.to_string());

                    Ok(AllocationSubmissionResult::new(Ok(id.to_string()), dir))
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
        workers_per_alloc: u32,
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
    }

    impl QueueBuilder {
        fn build(self) -> (QueueInfo, RateLimiter) {
            let Queue {
                manager,
                backlog,
                workers_per_alloc,
                timelimit,
                max_worker_count,
                limiter_max_alloc_fails,
                limiter_max_submit_fails,
                limiter_delays,
            } = self.finish().unwrap();
            (
                QueueInfo::new(
                    manager,
                    backlog,
                    workers_per_alloc,
                    timelimit,
                    vec![],
                    max_worker_count,
                    vec![],
                    None,
                    None,
                    None,
                ),
                RateLimiter::new(
                    limiter_delays,
                    limiter_max_submit_fails,
                    limiter_max_alloc_fails,
                ),
            )
        }
    }

    fn add_queue(
        autoalloc: &mut AutoAllocState,
        handler: Box<dyn QueueHandler>,
        queue_builder: QueueBuilder,
    ) -> QueueId {
        let (queue_info, limiter) = queue_builder.build();
        let queue = AllocationQueue::new(queue_info, None, handler, limiter);
        autoalloc.add_queue(queue, None)
    }

    fn new_hq_state(waiting_tasks: u32) -> StateRef {
        let state = create_hq_state();
        if waiting_tasks > 0 {
            attach_job(
                &mut state.get_mut(),
                0,
                waiting_tasks,
                Duration::from_secs(0),
            );
        }
        state
    }

    fn create_job(job_id: u32, tasks: u32, min_time: TimeRequest) -> Job {
        let def = ProgramDefinition {
            args: vec![],
            env: Default::default(),
            stdout: Default::default(),
            stderr: Default::default(),
            stdin: vec![],
            cwd: Default::default(),
        };
        let resources = ResourceRequestVariants::new_simple(ResourceRequest {
            n_nodes: 0,
            min_time,
            resources: smallvec![ResourceRequestEntry {
                resource: CPU_RESOURCE_NAME.to_string(),
                policy: AllocationRequest::Compact(ResourceAmount::new_units(1)),
            }],
        });

        let mut job = Job::new(
            job_id.into(),
            JobDescription {
                name: "job".to_string(),
                max_fails: None,
            },
            false,
        );
        job.attach_submit(SubmittedJobDescription::at(
            Utc::now(),
            JobSubmitDescription {
                task_desc: JobTaskDescription::Array {
                    ids: IntArray::from_range(0, tasks),
                    entries: None,
                    task_desc: TaskDescription {
                        kind: TaskKind::ExternalProgram(TaskKindProgram {
                            program: def,
                            pin_mode: PinMode::None,
                            task_dir: false,
                        }),
                        resources,
                        time_limit: None,
                        priority: 0,
                        crash_limit: CrashLimit::default(),
                    },
                },
                submit_dir: Default::default(),
                stream_path: None,
            },
        ));
        job
    }

    fn attach_job(state: &mut State, job_id: u32, tasks: u32, min_time: TimeRequest) {
        let job = create_job(job_id, tasks, min_time);
        state.add_job(job);
    }

    fn on_worker_added(
        streamer: &EventStreamer,
        queue_id: QueueId,
        state: &mut AutoAllocState,
        allocation_id: &str,
        worker_id: u32,
    ) {
        sync_allocation_status(
            streamer,
            queue_id,
            state.get_queue_mut(queue_id).unwrap(),
            allocation_id,
            AllocationSyncReason::WorkedConnected(worker_id.into()),
        );
    }

    fn on_worker_lost(
        streamer: &EventStreamer,
        queue_id: QueueId,
        state: &mut AutoAllocState,
        allocation_id: &str,
        worker_id: u32,
        details: LostWorkerDetails,
    ) {
        sync_allocation_status(
            streamer,
            queue_id,
            state.get_queue_mut(queue_id).unwrap(),
            allocation_id,
            AllocationSyncReason::WorkerLost(worker_id.into(), details),
        );
    }

    fn check_running_workers(allocation: &Allocation, workers: Vec<u32>) {
        let workers: Vec<WorkerId> = workers.into_iter().map(WorkerId::from).collect();
        match &allocation.status {
            AllocationState::Running {
                connected_workers, ..
            } => {
                let mut connected: Vec<WorkerId> = connected_workers.iter().cloned().collect();
                connected.sort_unstable();
                assert_eq!(connected, workers);
            }
            _ => panic!("Allocation should be in running status"),
        }
    }

    fn check_disconnected_running_workers(
        allocation: &Allocation,
        workers: Vec<(u32, LostWorkerReason)>,
    ) {
        let workers: Vec<(WorkerId, LostWorkerReason)> = workers
            .into_iter()
            .map(|(id, reason)| (WorkerId::from(id), reason))
            .collect();
        match &allocation.status {
            AllocationState::Running {
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
            _ => panic!("Allocation should be in running status"),
        }
    }

    fn check_finished_workers(allocation: &Allocation, workers: Vec<(u32, LostWorkerReason)>) {
        let workers: Vec<(WorkerId, LostWorkerReason)> = workers
            .into_iter()
            .map(|(id, reason)| (WorkerId::from(id), reason))
            .collect();
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

    fn check_queue_exists(autoalloc: &AutoAllocState, id: QueueId) {
        assert!(autoalloc.get_queue(id).is_some());
    }
    fn check_queue_paused(autoalloc: &AutoAllocState, id: QueueId) {
        assert!(matches!(
            autoalloc.get_queue(id).unwrap().state(),
            AllocationQueueState::Paused
        ));
    }

    fn fail_allocation(queue_id: QueueId, autoalloc: &mut AutoAllocState, allocation_id: &str) {
        let s = EventStreamer::new(None);
        on_worker_added(&s, queue_id, autoalloc, allocation_id, 0);
        on_worker_lost(
            &s,
            queue_id,
            autoalloc,
            allocation_id,
            0,
            lost_worker_quick(LostWorkerReason::ConnectionLost),
        );
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
