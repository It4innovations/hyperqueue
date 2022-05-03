use std::future::Future;
use std::path::PathBuf;
use std::time::SystemTime;

use futures::future::join_all;
use tempdir::TempDir;

use tako::common::{Map, Set};

use tako::messages::gateway::LostWorkerReason;
use tako::WorkerId;

use crate::common::manager::info::{ManagerInfo, ManagerType};
use crate::common::rpc::RpcReceiver;

use crate::get_or_return;
use crate::server::autoalloc::config::{
    get_refresh_timeout, get_status_check_interval, MAX_ALLOCATION_FAILS, MAX_SUBMISSION_FAILS,
    SUBMISSION_DELAYS,
};
use crate::server::autoalloc::queue::pbs::PbsHandler;
use crate::server::autoalloc::queue::slurm::SlurmHandler;
use crate::server::autoalloc::queue::{AllocationExternalStatus, QueueHandler, SubmitMode};
use crate::server::autoalloc::service::AutoAllocMessage;
use crate::server::autoalloc::state::{
    AllocationState, AutoAllocState, QueueState, RateLimiter, RateLimiterStatus,
};
use crate::server::autoalloc::{Allocation, AllocationId, AutoAllocResult, QueueId, QueueInfo};
use crate::server::job::Job;
use crate::server::state::{State, StateRef};
use crate::transfer::messages::{
    AllocationQueueParams, JobDescription, QueueData, TaskDescription,
};

pub async fn autoalloc_process(
    state_ref: StateRef,
    mut autoalloc: AutoAllocState,
    mut receiver: RpcReceiver<AutoAllocMessage>,
) {
    let timeout = get_refresh_timeout();
    loop {
        match tokio::time::timeout(timeout, receiver.recv()).await {
            Ok(Some(message)) => {
                let queue_id = handle_message(&mut autoalloc, &state_ref, message).await;
                refresh_state(&state_ref, &mut autoalloc, queue_id).await;
            }
            Ok(None) => break,
            Err(_) => {
                log::debug!(
                    "No message received in {} ms, refreshing state",
                    timeout.as_millis()
                );
                refresh_state(&state_ref, &mut autoalloc, None).await;
            }
        }
    }
    stop_all_allocations(&autoalloc).await;
}

/// Reacts to auto allocation message and returns a queue that should be refreshed.
/// If `None` is returned, all queues will be refreshed.
async fn handle_message(
    autoalloc: &mut AutoAllocState,
    state_ref: &StateRef,
    message: AutoAllocMessage,
) -> Option<QueueId> {
    log::debug!("Handling message {message:?}");
    match message {
        AutoAllocMessage::WorkerConnected(id, manager_info) => {
            log::debug!(
                "Registering worker {id} for allocation {}",
                manager_info.allocation_id
            );
            on_worker_connected(state_ref, autoalloc, id, &manager_info);
            autoalloc.get_queue_id_by_allocation(&manager_info.allocation_id)
        }
        AutoAllocMessage::WorkerLost(id, manager_info, reason) => {
            log::debug!(
                "Removing worker {id} from allocation {}",
                manager_info.allocation_id
            );
            on_worker_lost(state_ref, autoalloc, id, &manager_info, reason);
            autoalloc.get_queue_id_by_allocation(&manager_info.allocation_id)
        }
        AutoAllocMessage::JobCreated(_) => None,
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
                        },
                    )
                })
                .collect();
            response.respond(queues);
            None
        }
        AutoAllocMessage::AddQueue {
            server_directory,
            manager,
            params,
            response,
        } => {
            let result = create_queue(autoalloc, state_ref, server_directory, manager, params);
            let queue_id = result.as_ref().ok().copied();
            response.respond(result);
            queue_id
        }
        AutoAllocMessage::RemoveQueue {
            id,
            force,
            response,
        } => {
            let result = remove_queue(autoalloc, state_ref, id, force).await;
            response.respond(result);
            None
        }
        AutoAllocMessage::GetAllocations(queue_id, response) => {
            let result = match autoalloc.get_queue(queue_id) {
                Some(queue) => Ok(queue.all_allocations().cloned().collect()),
                None => Err(anyhow::anyhow!("Queue {queue_id} not found")),
            };
            response.respond(result);
            None
        }
    }
}

pub async fn try_submit_allocation(
    manager: ManagerType,
    params: AllocationQueueParams,
) -> anyhow::Result<()> {
    let tmpdir = TempDir::new("hq")?;
    let mut handler =
        create_allocation_handler(&manager, params.name.clone(), tmpdir.as_ref().to_path_buf())?;
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
        name: _name,
        workers_per_alloc,
        backlog,
        timelimit,
        additional_args,
        worker_cpu_arg,
        worker_resources_args,
        max_worker_count,
        on_server_lost,
    } = params;
    QueueInfo::new(
        backlog,
        workers_per_alloc,
        timelimit,
        on_server_lost,
        additional_args,
        worker_cpu_arg,
        worker_resources_args,
        max_worker_count,
    )
}

fn create_rate_limiter() -> RateLimiter {
    RateLimiter::new(
        SUBMISSION_DELAYS.to_vec(),
        MAX_SUBMISSION_FAILS,
        MAX_ALLOCATION_FAILS,
        get_status_check_interval(),
    )
}

fn create_queue(
    autoalloc: &mut AutoAllocState,
    state_ref: &StateRef,
    server_directory: PathBuf,
    manager: ManagerType,
    params: AllocationQueueParams,
) -> anyhow::Result<QueueId> {
    let name = params.name.clone();
    let handler = create_allocation_handler(&manager, name.clone(), server_directory);
    let queue_info = create_queue_info(params.clone());

    match handler {
        Ok(handler) => {
            let queue = QueueState::new(manager, queue_info, name, handler, create_rate_limiter());
            let id = {
                let id = autoalloc.add_queue(queue);
                state_ref
                    .get_mut()
                    .event_storage_mut()
                    .on_allocation_queue_created(id, params);
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
    state_ref: &StateRef,
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

    state_ref
        .get_mut()
        .event_storage_mut()
        .on_allocation_queue_removed(id);

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

/// Processes updates either for a single queue or for all queues and removes stale directories
/// from disk.
async fn refresh_state(
    state: &StateRef,
    autoalloc: &mut AutoAllocState,
    queue_id: Option<QueueId>,
) {
    let queue_ids = queue_id
        .map(|id| vec![id])
        .unwrap_or_else(|| autoalloc.queue_ids().collect());

    for id in queue_ids {
        refresh_queue_allocations(state, autoalloc, id).await;
        process_queue(state, autoalloc, id).await;
    }

    remove_inactive_directories(autoalloc).await;
}

async fn process_queue(state: &StateRef, autoalloc: &mut AutoAllocState, id: QueueId) {
    let submission_allowed = {
        let queue = get_or_return!(autoalloc.get_queue_mut(id));
        let limiter = queue.limiter_mut();

        let allowed = matches!(limiter.submission_status(), RateLimiterStatus::Ok);
        if allowed {
            limiter.on_submission_attempt();
        }
        allowed
    };

    if submission_allowed {
        queue_try_submit(id, autoalloc, state).await;
    }
    try_remove_queue(autoalloc, id).await;
}

fn get_data_from_worker<'a>(
    state: &'a mut AutoAllocState,
    id: WorkerId,
    manager_info: &ManagerInfo,
) -> Option<(&'a mut QueueState, QueueId, AllocationId)> {
    let allocation_id = &manager_info.allocation_id;
    match state.get_queue_id_by_allocation(allocation_id) {
        Some(queue_id) => state
            .get_queue_mut(queue_id)
            .map(|queue| (queue, queue_id, allocation_id.clone())),
        None => {
            log::warn!("Worker {id} belongs to an unknown allocation {allocation_id}");
            None
        }
    }
}

/// Synchronize the state of allocations with the external job manager.
async fn refresh_queue_allocations(
    state_ref: &StateRef,
    autoalloc: &mut AutoAllocState,
    id: QueueId,
) {
    log::debug!("Attempt to refresh allocations of queue {id}");
    let queue = get_or_return!(autoalloc.get_queue_mut(id));
    if !queue.limiter().can_perform_status_check() {
        log::debug!("Refresh attempt was rate limited");
        return;
    }

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

    queue.limiter_mut().on_status_attempt();
    let result = status_fut.await;

    log::debug!("Allocations of {id} have been refreshed: {result:?}");

    match result {
        Ok(mut status_map) => {
            let queue = get_or_return!(autoalloc.get_queue_mut(id));
            for allocation_id in allocation_ids {
                let status = status_map.remove(&allocation_id).unwrap_or_else(|| {
                    Ok(AllocationExternalStatus::Failed {
                        started_at: None,
                        finished_at: SystemTime::now(),
                    })
                });
                match status {
                    Ok(status) => match status {
                        AllocationExternalStatus::Failed { .. }
                        | AllocationExternalStatus::Finished { .. } => sync_allocation_status(
                            state_ref,
                            id,
                            queue,
                            &allocation_id,
                            Some(status),
                        ),
                        _ => {}
                    },
                    Err(error) => {
                        log::warn!("Could not get status of allocation {allocation_id}: {error:?}")
                    }
                }
            }
        }
        Err(error) => {
            log::error!("Failed to get allocations status from queue {id}: {error:?}",);
        }
    }
}

// Event reactors
fn on_worker_connected(
    state_ref: &StateRef,
    state: &mut AutoAllocState,
    worker_id: WorkerId,
    manager_info: &ManagerInfo,
) {
    let (queue, queue_id, allocation_id) =
        get_or_return!(get_data_from_worker(state, worker_id, manager_info));
    log::debug!("Worker {worker_id} connected to allocation {allocation_id}");

    let allocation = get_or_return!(queue.get_allocation_mut(&allocation_id));
    match allocation.status {
        AllocationState::Queued => {
            allocation.status = AllocationState::Running {
                connected_workers: Set::from_iter([worker_id]),
                disconnected_workers: Default::default(),
                started_at: SystemTime::now(),
            };
            state_ref
                .get_mut()
                .event_storage_mut()
                .on_allocation_started(queue_id, allocation_id.clone());
        }
        AllocationState::Running {
            ref mut connected_workers,
            ..
        } => {
            if allocation.target_worker_count == connected_workers.len() as u64 {
                log::warn!("Allocation {allocation_id} already has the expected number of workers, worker {worker_id} is not expected");
            }
            if !connected_workers.insert(worker_id) {
                log::warn!("Allocation {allocation_id} already had worker {worker_id} connected");
            }
        }
        _ => {
            log::warn!(
                "Allocation {allocation_id} has status {:?} and does not expect new workers",
                allocation.status
            );
        }
    }
}

fn on_worker_lost(
    state_ref: &StateRef,
    state: &mut AutoAllocState,
    worker_id: WorkerId,
    manager_info: &ManagerInfo,
    reason: LostWorkerReason,
) {
    let (queue, queue_id, allocation_id) =
        get_or_return!(get_data_from_worker(state, worker_id, manager_info));
    let allocation = get_or_return!(queue.get_allocation_mut(&allocation_id));

    match allocation.status {
        AllocationState::Queued => {
            log::warn!(
                "Worker {worker_id} has disconnected before it has connected to an allocation queue!"
            );
        }
        AllocationState::Running {
            ref mut disconnected_workers,
            ref mut connected_workers,
            ..
        } => {
            if !connected_workers.remove(&worker_id) {
                log::warn!("Worker {worker_id} has disconnected multiple times!");
            }
            disconnected_workers.insert(worker_id, reason);
            sync_allocation_status(state_ref, queue_id, queue, &allocation_id, None);
        }
        AllocationState::Finished { .. } => {
            log::warn!(
                "Worker {worker_id} has disconnected from an already finished allocation {}",
                allocation.id
            );
        }
        AllocationState::Invalid { .. } => {
            log::warn!(
                "Worker {worker_id} has disconnected from an invalid allocation {}",
                allocation.id
            );
        }
    }
}

fn sync_allocation_status(
    state_ref: &StateRef,
    queue_id: QueueId,
    queue: &mut QueueState,
    allocation_id: &str,
    external_status: Option<AllocationExternalStatus>,
) {
    let allocation = get_or_return!(queue.get_allocation_mut(allocation_id));

    enum Action {
        Failure,
        Success,
    }

    let action: Option<Action> = {
        // Unexpected end of allocation
        if let Some(status) = external_status {
            let (connected, disconnected) = match &mut allocation.status {
                AllocationState::Queued => (Default::default(), Default::default()),
                AllocationState::Running {
                    connected_workers,
                    disconnected_workers,
                    ..
                } => (
                    std::mem::take(connected_workers),
                    std::mem::take(disconnected_workers),
                ),
                AllocationState::Finished { .. } | AllocationState::Invalid { .. } => {
                    // The allocation was already finished before
                    return;
                }
            };
            let failed = matches!(status, AllocationExternalStatus::Failed { .. });
            match status {
                AllocationExternalStatus::Finished {
                    started_at,
                    finished_at,
                }
                | AllocationExternalStatus::Failed {
                    started_at,
                    finished_at,
                } => {
                    log::debug!("Setting allocation {allocation_id} status to invalid because of external status {status:?}.");
                    allocation.status = AllocationState::Invalid {
                        connected_workers: connected,
                        started_at,
                        finished_at,
                        disconnected_workers: disconnected,
                        failed,
                    };
                    Some(if failed {
                        Action::Failure
                    } else {
                        Action::Success
                    })
                }
                _ => None,
            }
        } else {
            match &mut allocation.status {
                AllocationState::Running {
                    disconnected_workers,
                    started_at,
                    ..
                } => {
                    // All expected workers have disconnected, the allocation ends in a normal way
                    if disconnected_workers.len() as u64 == allocation.target_worker_count {
                        let is_failed = disconnected_workers
                            .values()
                            .any(|reason| reason.is_failure());

                        allocation.status = AllocationState::Finished {
                            started_at: *started_at,
                            finished_at: SystemTime::now(),
                            disconnected_workers: std::mem::take(disconnected_workers),
                        };

                        Some(if is_failed {
                            Action::Failure
                        } else {
                            Action::Success
                        })
                    } else {
                        None
                    }
                }
                AllocationState::Invalid { .. }
                | AllocationState::Finished { .. }
                | AllocationState::Queued => None,
            }
        }
    };

    match action {
        Some(Action::Success) => queue.limiter_mut().on_allocation_success(),
        Some(Action::Failure) => queue.limiter_mut().on_allocation_fail(),
        None => {}
    }
    if action.is_some() {
        state_ref
            .get_mut()
            .event_storage_mut()
            .on_allocation_finished(queue_id, allocation_id.to_string());
    }
}

async fn queue_try_submit(queue_id: QueueId, autoalloc: &mut AutoAllocState, state_ref: &StateRef) {
    let (allocations_to_create, workers_per_alloc, mut waiting_tasks, mut available_workers) = {
        let queue = get_or_return!(autoalloc.get_queue(queue_id));

        let allocs_in_queue = queue.queued_allocations().count();

        let info = queue.info();
        let waiting_tasks = count_available_tasks(&state_ref.get(), info);
        let active_workers = count_active_workers(queue);
        let available_workers = match info.max_worker_count() {
            Some(max) => (max as u64).saturating_sub(active_workers),
            None => u64::MAX,
        };

        (
            info.backlog().saturating_sub(allocs_in_queue as u32),
            info.workers_per_alloc() as u64,
            waiting_tasks,
            available_workers,
        )
    };

    for _ in 0..allocations_to_create {
        // If there are no more waiting tasks, stop creating allocations
        // Assume that each worker will handle at least a single task
        if waiting_tasks == 0 {
            log::debug!("No more waiting tasks found, no new allocations will be created");
            break;
        }
        // If the worker limit was reached, stop creating new allocations
        if available_workers == 0 {
            log::debug!("Worker limit reached, no new allocations will be created");
            break;
        }

        let workers_to_spawn = std::cmp::min(workers_per_alloc, available_workers);
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

        let mut state = state_ref.get_mut();
        let event_manager = state.event_storage_mut();
        match result {
            Ok(submission_result) => {
                let working_dir = submission_result.working_dir().to_path_buf();
                match submission_result.into_id() {
                    Ok(allocation_id) => {
                        log::info!("Queued {workers_to_spawn} worker(s) into queue {queue_id}: {allocation_id}");
                        event_manager.on_allocation_queued(
                            queue_id,
                            allocation_id.clone(),
                            workers_to_spawn,
                        );
                        let allocation =
                            Allocation::new(allocation_id, workers_to_spawn, working_dir);
                        autoalloc.add_allocation(allocation, queue_id);
                        let queue = get_or_return!(autoalloc.get_queue_mut(queue_id));
                        queue.limiter_mut().on_submission_success();

                        waiting_tasks = waiting_tasks.saturating_sub(workers_to_spawn);
                        available_workers = available_workers.saturating_sub(workers_to_spawn);
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

async fn remove_inactive_directories(autoalloc: &mut AutoAllocState) {
    let to_remove = autoalloc.get_directories_for_removal();
    let futures = to_remove.into_iter().map(|dir| async move {
        let result = tokio::fs::remove_dir_all(&dir).await;
        (result, dir)
    });
    for (result, directory) in join_all(futures).await {
        if let Err(err) = result {
            log::error!("Failed to remove stale allocation directory {directory:?}: {err:?}",);
        }
    }
}

async fn try_remove_queue(autoalloc: &mut AutoAllocState, id: QueueId) {
    let futures = {
        let queue = get_or_return!(autoalloc.get_queue_mut(id));
        let limiter = queue.limiter();

        let status = limiter.submission_status();
        match status {
            RateLimiterStatus::TooManyFailedSubmissions
            | RateLimiterStatus::TooManyFailedAllocations => {
                if let RateLimiterStatus::TooManyFailedSubmissions = status {
                    log::error!(
                        "The queue {id} had too many failed submissions, it will be removed."
                    );
                } else {
                    log::error!(
                        "The queue {id} had too many failed allocations, it will be removed."
                    );
                }
                let futures = prepare_queue_cleanup(queue);
                autoalloc.remove_queue(id);
                futures
            }
            RateLimiterStatus::Ok | RateLimiterStatus::Wait => return,
        }
    };
    futures::future::join_all(futures).await;
}

pub fn prepare_queue_cleanup(
    state: &QueueState,
) -> Vec<impl Future<Output = (AutoAllocResult<()>, AllocationId)>> {
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

fn can_provide_worker(job: &Job, queue_info: &QueueInfo) -> bool {
    match &job.job_desc {
        JobDescription::Array {
            task_desc: TaskDescription { resources, .. },
            ..
        } => resources.min_time < queue_info.timelimit(),
        JobDescription::Graph { .. } => {
            todo!()
        }
    }
}

fn count_available_tasks(state: &State, queue_info: &QueueInfo) -> u64 {
    let waiting_tasks: u64 = state
        .jobs()
        .filter(|job| !job.is_terminated() && can_provide_worker(job, queue_info))
        .map(|job| job.counters.n_waiting_tasks(job.n_tasks()) as u64)
        .sum();
    waiting_tasks
}

fn count_active_workers(queue: &QueueState) -> u64 {
    queue
        .active_allocations()
        .map(|allocation| allocation.target_worker_count)
        .sum()
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::time::{Duration, Instant};

    use anyhow::anyhow;
    use derive_builder::Builder;
    use tempdir::TempDir;

    use tako::common::resources::TimeRequest;
    use tako::common::{Map, Set, WrappedRcRefCell};
    use tako::messages::common::ProgramDefinition;
    use tako::messages::gateway::{LostWorkerReason, ResourceRequest};
    use tako::worker::state::ServerLostPolicy;
    use tako::WorkerId;

    use crate::common::arraydef::IntArray;
    use crate::common::manager::info::{ManagerInfo, ManagerType};
    use crate::common::utils::time::mock_time::MockTime;
    use crate::server::autoalloc::process::{
        on_worker_connected, on_worker_lost, queue_try_submit, refresh_state,
    };
    use crate::server::autoalloc::queue::{
        AllocationExternalStatus, AllocationStatusMap, AllocationSubmissionResult, QueueHandler,
        SubmitMode,
    };
    use crate::server::autoalloc::state::{
        AllocationState, AutoAllocState, QueueState, RateLimiter,
    };
    use crate::server::autoalloc::{Allocation, AllocationId, AutoAllocResult, QueueId, QueueInfo};
    use crate::server::job::Job;
    use crate::server::state::StateRef;
    use crate::tests::utils::create_hq_state;
    use crate::transfer::messages::{JobDescription, PinMode, TaskDescription};

    #[tokio::test]
    async fn fill_backlog() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(4).workers_per_alloc(2),
        );

        queue_try_submit(queue_id, &mut state, &hq_state).await;

        let allocations = get_allocations(&state, queue_id);
        assert_eq!(allocations.len(), 4);
        assert!(allocations
            .iter()
            .all(|alloc| alloc.target_worker_count == 2));
    }

    #[tokio::test]
    async fn do_nothing_on_full_backlog() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(&mut state, handler, QueueBuilder::default().backlog(4));

        for _ in 0..5 {
            queue_try_submit(queue_id, &mut state, &hq_state).await;
        }

        assert_eq!(get_allocations(&state, queue_id).len(), 4);
    }

    #[tokio::test]
    async fn worker_connects_from_unknown_allocation() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(1),
        );

        queue_try_submit(queue_id, &mut state, &hq_state).await;
        on_worker_connected(&hq_state, &mut state, 0.into(), &create_worker("foo"));
        assert!(get_allocations(&state, queue_id)
            .iter()
            .all(|alloc| !alloc.is_running()));
    }

    #[tokio::test]
    async fn start_allocation_when_worker_connects() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(1),
        );

        queue_try_submit(queue_id, &mut state, &hq_state).await;
        let allocs = get_allocations(&state, queue_id);

        on_worker_connected(
            &hq_state,
            &mut state,
            10.into(),
            &create_worker(&allocs[0].id),
        );
        check_running_workers(get_allocations(&state, queue_id).get(0).unwrap(), vec![10]);
    }

    #[tokio::test]
    async fn add_another_worker_to_allocation() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(2),
        );

        queue_try_submit(queue_id, &mut state, &hq_state).await;
        let allocs = get_allocations(&state, queue_id);

        for id in [0, 1] {
            on_worker_connected(
                &hq_state,
                &mut state,
                id.into(),
                &create_worker(&allocs[0].id),
            );
        }
        check_running_workers(
            get_allocations(&state, queue_id).get(0).unwrap(),
            vec![0, 1],
        );
    }

    #[tokio::test]
    async fn finish_allocation_when_worker_disconnects() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(1),
        );

        queue_try_submit(queue_id, &mut state, &hq_state).await;
        let allocs = get_allocations(&state, queue_id);

        let worker_id: WorkerId = 0.into();
        on_worker_connected(
            &hq_state,
            &mut state,
            worker_id,
            &create_worker(&allocs[0].id),
        );
        on_worker_lost(
            &hq_state,
            &mut state,
            worker_id,
            &create_worker(&allocs[0].id),
            LostWorkerReason::ConnectionLost,
        );
        check_finished_workers(
            get_allocations(&state, queue_id).get(0).unwrap(),
            vec![(0, LostWorkerReason::ConnectionLost)],
        );
    }

    #[tokio::test]
    async fn finish_allocation_when_last_worker_disconnects() {
        let hq_state = new_hq_state(1000);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(1).workers_per_alloc(2),
        );

        queue_try_submit(queue_id, &mut state, &hq_state).await;
        let allocs = get_allocations(&state, queue_id);

        for id in [0, 1] {
            on_worker_connected(
                &hq_state,
                &mut state,
                id.into(),
                &create_worker(&allocs[0].id),
            );
        }
        on_worker_lost(
            &hq_state,
            &mut state,
            0.into(),
            &create_worker(&allocs[0].id),
            LostWorkerReason::ConnectionLost,
        );
        let allocation = get_allocations(&state, queue_id)[0].clone();
        check_running_workers(&allocation, vec![1]);
        check_disconnected_running_workers(
            &allocation,
            vec![(0, LostWorkerReason::ConnectionLost)],
        );
        on_worker_lost(
            &hq_state,
            &mut state,
            1.into(),
            &create_worker(&allocs[0].id),
            LostWorkerReason::HeartbeatLost,
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
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(&mut state, handler, QueueBuilder::default().backlog(3));

        queue_try_submit(queue_id, &mut state, &hq_state).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 0);
    }

    #[tokio::test]
    async fn do_not_fill_backlog_when_tasks_run_out() {
        let hq_state = new_hq_state(5);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(5).workers_per_alloc(2),
        );

        // 5 tasks, 3 * 2 workers -> last two allocations should be ignored
        queue_try_submit(queue_id, &mut state, &hq_state).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 3);
    }

    #[tokio::test]
    async fn stop_allocating_on_error() {
        let hq_state = new_hq_state(5);
        let mut state = AutoAllocState::new();

        let handler_state = WrappedRcRefCell::wrap(HandlerState::default());
        let handler = stateful_handler(handler_state.clone());
        let queue_id = add_queue(&mut state, handler, QueueBuilder::default().backlog(5));

        handler_state.get_mut().allocation_will_fail = true;

        // Only try the first allocation in the backlog
        queue_try_submit(queue_id, &mut state, &hq_state).await;
        assert_eq!(handler_state.get().allocation_attempts, 1);

        handler_state.get_mut().allocation_will_fail = false;

        // Finish the rest
        queue_try_submit(queue_id, &mut state, &hq_state).await;
        assert_eq!(handler_state.get().allocation_attempts, 6);
    }

    #[tokio::test]
    async fn ignore_task_with_high_time_request() {
        let hq_state = new_hq_state(0);
        hq_state
            .get_mut()
            .add_job(create_job(1, Duration::from_secs(60 * 60)));
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();
        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().timelimit(Duration::from_secs(60 * 30)),
        );

        // Allocations last for 30 minutes, but job requires 60 minutes
        // Nothing should be scheduled
        queue_try_submit(queue_id, &mut state, &hq_state).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 0);
    }

    #[tokio::test]
    async fn respect_max_worker_count() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new();

        let handler_state = WrappedRcRefCell::wrap(HandlerState::default());
        let handler = stateful_handler(handler_state.clone());

        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default().backlog(4).max_worker_count(Some(5)),
        );

        // Put 4 allocations into the queue.
        queue_try_submit(queue_id, &mut state, &hq_state).await;
        let allocations = get_allocations(&state, queue_id);
        assert_eq!(allocations.len(), 4);

        // Start 2 allocations
        on_worker_connected(
            &hq_state,
            &mut state,
            0.into(),
            &create_worker(&allocations[0].id),
        );
        on_worker_connected(
            &hq_state,
            &mut state,
            1.into(),
            &create_worker(&allocations[1].id),
        );

        // Create only one additional allocation
        queue_try_submit(queue_id, &mut state, &hq_state).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 5);

        // Finish one allocation
        on_worker_lost(
            &hq_state,
            &mut state,
            1.into(),
            &create_worker(&allocations[1].id),
            LostWorkerReason::ConnectionLost,
        );

        // One worker was freed, create an additional allocation
        queue_try_submit(queue_id, &mut state, &hq_state).await;
        assert_eq!(get_allocations(&state, queue_id).len(), 6);
    }

    #[tokio::test]
    async fn max_worker_count_shorten_last_allocation() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new();

        let handler = always_queued_handler();

        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default()
                .backlog(2)
                .workers_per_alloc(4)
                .max_worker_count(Some(6)),
        );

        queue_try_submit(queue_id, &mut state, &hq_state).await;
        let allocations = get_allocations(&state, queue_id);
        assert_eq!(allocations.len(), 2);
        assert_eq!(allocations[0].target_worker_count, 4);
        assert_eq!(allocations[1].target_worker_count, 2);
    }

    #[tokio::test]
    async fn delete_stale_directories_of_unsubmitted_allocations() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new();

        let make_dir = || {
            let tempdir = TempDir::new("hq").unwrap();
            tempdir.into_path()
        };

        let handler = fails_submit_handler();

        let max_kept = 2;
        state.set_max_kept_directories(max_kept);
        add_queue(&mut state, handler, QueueBuilder::default());

        let dirs = vec![make_dir(), make_dir()];
        state.set_inactive_allocation_directories(dirs.iter().cloned().collect());

        // Delete oldest directory
        refresh_state(&hq_state, &mut state, None).await;
        assert!(!dirs[0].exists());
        assert!(dirs[1].exists());

        // Delete second oldest directory
        refresh_state(&hq_state, &mut state, None).await;
        assert!(!dirs[1].exists());
    }

    #[tokio::test]
    async fn remove_queue_when_submission_fails_too_many_times() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new();

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

        refresh_state(&hq_state, &mut state, None).await;
        check_queue_exists(&state, queue_id);
        refresh_state(&hq_state, &mut state, None).await;
        check_queue_doesnt_exist(&state, queue_id);
    }

    #[tokio::test]
    async fn remove_queue_when_allocation_fails_too_many_times() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new();

        let shared = WrappedRcRefCell::wrap(HandlerState::default());
        let handler = stateful_handler(shared.clone());

        let queue_id = add_queue(
            &mut state,
            handler,
            QueueBuilder::default()
                .backlog(5)
                .limiter_max_alloc_fails(2),
        );

        queue_try_submit(queue_id, &mut state, &hq_state).await;

        let allocations = get_allocations(&state, queue_id);
        fail_allocation(&hq_state, &mut state, &allocations[0].id);
        refresh_state(&hq_state, &mut state, Some(queue_id)).await;
        check_queue_exists(&state, queue_id);

        fail_allocation(&hq_state, &mut state, &allocations[1].id);
        refresh_state(&hq_state, &mut state, Some(queue_id)).await;
        check_queue_doesnt_exist(&state, queue_id);
    }

    #[tokio::test]
    async fn respect_rate_limiter() {
        let hq_state = new_hq_state(100);
        let mut state = AutoAllocState::new();

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

        let mut now = Instant::now();
        {
            let _mock = MockTime::mock(now);
            refresh_state(&hq_state, &mut state, None).await;
            check_alloc_count(1);
        }

        // Now we should sleep until +1 sec
        {
            now += Duration::from_millis(500);
            let _mock = MockTime::mock(now);
            refresh_state(&hq_state, &mut state, None).await;
            check_alloc_count(1);
        }
        {
            now += Duration::from_millis(1500);
            let _mock = MockTime::mock(now);
            refresh_state(&hq_state, &mut state, None).await;
            check_alloc_count(2);
        }

        // Now we should sleep until +10 sec
        {
            now += Duration::from_millis(5000);
            let _mock = MockTime::mock(now);
            refresh_state(&hq_state, &mut state, None).await;
            check_alloc_count(2);
        }
        {
            now += Duration::from_millis(6000);
            let _mock = MockTime::mock(now);
            refresh_state(&hq_state, &mut state, None).await;
            check_alloc_count(3);
        }
        // The delay shouldn't increase any more when we have reached the maximum delay
        {
            now += Duration::from_millis(11000);
            let _mock = MockTime::mock(now);
            refresh_state(&hq_state, &mut state, None).await;
            check_alloc_count(4);
        }
    }

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
                let tempdir = TempDir::new("hq").unwrap();
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

    /// Handler that spawns allocations with sequentially increasing IDs (starting at *0*) and
    /// returns allocation statuses from [`HandlerState`].
    fn stateful_handler(state: WrappedRcRefCell<HandlerState>) -> Box<dyn QueueHandler> {
        Handler::new(
            state.clone(),
            move |state, _worker_count| async move {
                let mut state = state.get_mut();
                let tempdir = TempDir::new("hq").unwrap();
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
        #[builder(default = "1")]
        backlog: u32,
        #[builder(default = "1")]
        workers_per_alloc: u32,
        #[builder(default = "Duration::from_secs(60 * 60)")]
        timelimit: Duration,
        #[builder(default)]
        max_worker_count: Option<u32>,
        #[builder(default = "100")]
        limiter_max_alloc_fails: usize,
        #[builder(default = "100")]
        limiter_max_submit_fails: usize,
        #[builder(default = "vec![Duration::ZERO]")]
        limiter_delays: Vec<Duration>,
    }

    impl QueueBuilder {
        fn build(self) -> (QueueInfo, RateLimiter) {
            let Queue {
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
                    backlog,
                    workers_per_alloc,
                    timelimit,
                    ServerLostPolicy::Stop,
                    vec![],
                    None,
                    vec![],
                    max_worker_count,
                ),
                RateLimiter::new(
                    limiter_delays,
                    limiter_max_submit_fails,
                    limiter_max_alloc_fails,
                    Duration::from_secs(1),
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
        let queue = QueueState::new(ManagerType::Pbs, queue_info, None, handler, limiter);
        autoalloc.add_queue(queue)
    }

    fn new_hq_state(waiting_tasks: u32) -> StateRef {
        let state = create_hq_state();
        if waiting_tasks > 0 {
            state
                .get_mut()
                .add_job(create_job(waiting_tasks, Duration::from_secs(0)));
        }
        state
    }

    fn get_allocations(autoalloc: &AutoAllocState, queue_id: QueueId) -> Vec<Allocation> {
        let mut allocations: Vec<_> = autoalloc
            .get_queue(queue_id)
            .unwrap()
            .all_allocations()
            .cloned()
            .collect();
        allocations.sort_by(|a, b| a.id.cmp(&b.id));
        allocations
    }

    fn create_job(tasks: u32, min_time: TimeRequest) -> Job {
        let def = ProgramDefinition {
            args: vec![],
            env: Default::default(),
            stdout: Default::default(),
            stderr: Default::default(),
            stdin: vec![],
            cwd: Default::default(),
        };
        let resources = ResourceRequest {
            n_nodes: 0,
            cpus: Default::default(),
            generic: vec![],
            min_time,
        };

        Job::new(
            JobDescription::Array {
                ids: IntArray::from_range(0, tasks),
                entries: None,
                task_desc: TaskDescription {
                    program: def,
                    resources: resources.clone(),
                    pin_mode: PinMode::None,
                    task_dir: false,
                    time_limit: None,
                    priority: 0,
                },
            },
            0.into(),
            0.into(),
            "job".to_string(),
            None,
            None,
            Default::default(),
        )
    }
    fn create_worker(allocation_id: &str) -> ManagerInfo {
        ManagerInfo {
            manager: ManagerType::Pbs,
            allocation_id: allocation_id.to_string(),
            time_limit: Some(Duration::from_secs(60 * 60)),
        }
    }

    fn check_running_workers(allocation: &Allocation, workers: Vec<u32>) {
        let workers: Vec<WorkerId> = workers.into_iter().map(|id| WorkerId::from(id)).collect();
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
                let mut disconnected: Vec<_> = disconnected_workers.clone().into_iter().collect();
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
                let mut disconnected: Vec<_> = disconnected_workers.clone().into_iter().collect();
                disconnected.sort_unstable_by_key(|(id, _)| *id);
                assert_eq!(disconnected, workers);
            }
            _ => panic!("Allocation should be in finished status"),
        }
    }

    fn check_queue_exists(autoalloc: &AutoAllocState, id: QueueId) {
        assert!(autoalloc.get_queue(id).is_some());
    }
    fn check_queue_doesnt_exist(autoalloc: &AutoAllocState, id: QueueId) {
        assert!(autoalloc.get_queue(id).is_none());
    }

    fn fail_allocation(hq_state: &StateRef, autoalloc: &mut AutoAllocState, allocation_id: &str) {
        let minfo = create_worker(allocation_id);
        let worker_id: WorkerId = 0.into();
        on_worker_connected(hq_state, autoalloc, worker_id, &minfo);
        on_worker_lost(
            &hq_state,
            autoalloc,
            worker_id,
            &minfo,
            LostWorkerReason::ConnectionLost,
        );
    }
}
