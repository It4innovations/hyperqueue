use futures::future::join_all;
use std::time::SystemTime;

use crate::server::autoalloc::state::{
    Allocation, AllocationEvent, AllocationStatus, DescriptorState,
};
use crate::server::autoalloc::{DescriptorId, QueueInfo};
use crate::server::job::Job;
use crate::server::state::{State, StateRef};
use crate::transfer::messages::{JobDescription, TaskDescription};

macro_rules! get_or_return {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            _ => return,
        }
    };
}

macro_rules! get_or_continue {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            _ => continue,
        }
    };
}

/// The main entrypoint of the autoalloc background process.
/// It invokes the autoalloc logic in fixed time intervals.
pub async fn autoalloc_process(state_ref: StateRef) {
    let duration = state_ref.get().get_autoalloc_state().refresh_interval();
    let mut interval = tokio::time::interval(duration);
    loop {
        interval.tick().await;
        autoalloc_tick(&state_ref).await;
    }
}

/// Removes all remaining active allocations
pub async fn autoalloc_shutdown(state_ref: StateRef) {
    let futures: Vec<_> = {
        state_ref
            .get()
            .get_autoalloc_state()
            .descriptors()
            .flat_map(|(_, descriptor)| {
                let handler = descriptor.descriptor.handler();
                descriptor
                    .all_allocations()
                    .filter(|alloc| alloc.is_active())
                    .map(move |alloc| {
                        let fut = handler.remove_allocation(alloc);
                        let id = alloc.id.clone();
                        async move { (fut.await, id) }
                    })
            })
            .collect()
    };

    for (result, allocation_id) in futures::future::join_all(futures).await {
        match result {
            Ok(_) => {
                log::info!("Allocation {} was removed", allocation_id);
            }
            Err(e) => {
                log::error!("Failed to remove allocation {}: {:?}", allocation_id, e);
            }
        }
    }
}

async fn autoalloc_tick(state_ref: &StateRef) {
    log::debug!("Running autoalloc");

    let futures: Vec<_> = state_ref
        .get()
        .get_autoalloc_state()
        .descriptor_ids()
        .map(|id| process_descriptor(id, state_ref))
        .collect();
    futures::future::join_all(futures).await;
}

async fn process_descriptor(id: DescriptorId, state: &StateRef) {
    // TODO: check only once in a while
    refresh_allocations(id, state).await;
    schedule_new_allocations(id, state).await
}

/// Go through the allocations of descriptor with the given name and refresh their status.
/// Queue allocations might become running or finished, running allocations might become finished,
/// etc.
#[allow(clippy::needless_collect)]
async fn refresh_allocations(id: DescriptorId, state_ref: &StateRef) {
    let allocation_ids: Vec<_> =
        get_or_return!(state_ref.get().get_autoalloc_state().get_descriptor(id))
            .active_allocations()
            .map(|alloc| alloc.id.clone())
            .collect();
    for allocation_id in allocation_ids.into_iter() {
        let status_fut = {
            let state = state_ref.get();
            let descriptor = get_or_return!(state.get_autoalloc_state().get_descriptor(id));
            let allocation = get_or_continue!(descriptor.get_allocation(&allocation_id));
            descriptor
                .descriptor
                .handler()
                .get_allocation_status(allocation)
        };

        let result = status_fut.await;

        let mut state = state_ref.get_mut();
        let (alloc_state, event_manager) = state.split_autoalloc_events_mut();
        match result {
            Ok(status) => {
                let descriptor = get_or_continue!(alloc_state.get_descriptor_mut(id));
                let working_dir = get_or_continue!(descriptor.get_allocation(&allocation_id))
                    .working_dir
                    .clone();

                match status {
                    Some(status) => {
                        let id = allocation_id.clone();
                        log::debug!("Status of allocation {}: {:?}", allocation_id, status);
                        match status {
                            AllocationStatus::Running { .. } => {
                                let allocation =
                                    get_or_continue!(descriptor.get_allocation_mut(&allocation_id));
                                if let AllocationStatus::Queued = allocation.status {
                                    event_manager.on_allocation_started(allocation_id.clone());
                                    descriptor.add_event(AllocationEvent::AllocationStarted(
                                        allocation_id,
                                    ));
                                }
                            }
                            AllocationStatus::Finished { .. } => {
                                event_manager.on_allocation_finished(allocation_id.clone());
                                descriptor
                                    .add_event(AllocationEvent::AllocationFinished(allocation_id));
                                descriptor.add_inactive_directory(working_dir);
                            }
                            AllocationStatus::Failed { .. } => {
                                event_manager.on_allocation_finished(allocation_id.clone());
                                descriptor
                                    .add_event(AllocationEvent::AllocationFailed(allocation_id));
                                descriptor.add_inactive_directory(working_dir);
                            }
                            AllocationStatus::Queued => {}
                        };
                        get_or_continue!(descriptor.get_allocation_mut(&id)).status = status;
                    }
                    None => {
                        log::warn!("Allocation {} was not found", allocation_id);
                        descriptor.remove_allocation(&allocation_id);
                        descriptor.add_event(AllocationEvent::AllocationDisappeared(allocation_id));
                        descriptor.add_inactive_directory(working_dir);
                    }
                };
            }
            Err(err) => {
                log::error!(
                    "Failed to get allocation {} status from {}: {:?}",
                    allocation_id,
                    id,
                    err
                );
                let descriptor = get_or_continue!(alloc_state.get_descriptor_mut(id));
                descriptor.add_event(AllocationEvent::StatusFail {
                    error: format!("{:?}", err),
                });
            }
        }
    }
}

/// Find out if workers spawned in this queue can possibly provide computational resources
/// for tasks of this job.
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

fn count_active_workers(descriptor: &DescriptorState) -> u64 {
    descriptor
        .active_allocations()
        .map(|allocation| allocation.worker_count)
        .sum()
}

/// Schedule new allocations for the descriptor with the given name.
async fn schedule_new_allocations(id: DescriptorId, state_ref: &StateRef) {
    let (allocations_to_create, workers_per_alloc, mut waiting_tasks, mut available_workers) = {
        let state = state_ref.get();
        let descriptor = get_or_return!(state.get_autoalloc_state().get_descriptor(id));
        let allocs_in_queue = descriptor.queued_allocations().count();

        let waiting_tasks = count_available_tasks(&state, descriptor.descriptor.info());
        let active_workers = count_active_workers(descriptor);
        let info = descriptor.descriptor.info();
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
            let mut state = state_ref.get_mut();
            let descriptor = get_or_return!(state.get_autoalloc_state_mut().get_descriptor_mut(id));
            let info = descriptor.descriptor.info().clone();
            descriptor
                .descriptor
                .handler_mut()
                .submit_allocation(id, &info, workers_to_spawn)
        };

        let result = schedule_fut.await;

        let mut state = state_ref.get_mut();
        let (alloc_state, event_manager) = state.split_autoalloc_events_mut();
        let descriptor = get_or_return!(alloc_state.get_descriptor_mut(id));
        match result {
            Ok(submission_result) => {
                let working_dir = submission_result.working_dir().to_path_buf();
                match submission_result.into_id() {
                    Ok(allocation_id) => {
                        log::info!("Queued {} workers into queue {}", workers_to_spawn, id);
                        event_manager.on_allocation_queued(allocation_id.clone(), workers_to_spawn);
                        descriptor
                            .add_event(AllocationEvent::AllocationQueued(allocation_id.clone()));
                        descriptor.add_allocation(Allocation {
                            id: allocation_id,
                            worker_count: workers_to_spawn,
                            queued_at: SystemTime::now(),
                            status: AllocationStatus::Queued,
                            working_dir,
                        });

                        waiting_tasks = waiting_tasks.saturating_sub(workers_to_spawn);
                        available_workers = available_workers.saturating_sub(workers_to_spawn);
                    }
                    Err(err) => {
                        log::error!("Failed to submit allocation into queue {}: {:?}", id, err);
                        descriptor.add_event(AllocationEvent::QueueFail {
                            error: format!("{:?}", err),
                        });
                        descriptor.add_inactive_directory(working_dir);
                    }
                }
            }
            Err(err) => {
                log::error!(
                    "Failed to create allocation directory for queue {}: {:?}",
                    id,
                    err
                );
                descriptor.add_event(AllocationEvent::QueueFail {
                    error: format!("{:?}", err),
                });
            }
        }
    }

    remove_inactive_directories(id, state_ref).await;
}

/// Removes directories of inactive allocations scheduled for removal.
async fn remove_inactive_directories(id: DescriptorId, state_ref: &StateRef) {
    let to_remove = {
        let mut state = state_ref.get_mut();
        get_or_return!(state.get_autoalloc_state_mut().get_descriptor_mut(id))
            .get_directories_for_removal()
    };
    let futures: Vec<_> = to_remove
        .into_iter()
        .map(|dir| async move {
            let result = tokio::fs::remove_dir_all(&dir).await;
            (result, dir)
        })
        .collect();
    for (result, directory) in join_all(futures).await {
        if let Err(err) = result {
            log::error!(
                "Failed to remove stale allocation directory {:?}: {:?}",
                directory,
                err
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use derive_builder::Builder;
    use std::future::Future;
    use std::pin::Pin;
    use std::time::{Duration, SystemTime};
    use tako::common::Map;
    use tempdir::TempDir;

    use crate::common::arraydef::IntArray;
    use tako::common::resources::TimeRequest;
    use tako::messages::common::ProgramDefinition;
    use tako::messages::gateway::ResourceRequest;

    use crate::common::manager::info::ManagerType;
    use crate::server::autoalloc::descriptor::{
        AllocationSubmissionResult, QueueDescriptor, QueueHandler, QueueInfo,
    };
    use crate::server::autoalloc::process::autoalloc_tick;
    use crate::server::autoalloc::state::{AllocationEvent, AllocationId, AllocationStatus};
    use crate::server::autoalloc::{Allocation, AutoAllocResult, DescriptorId};
    use crate::server::job::Job;
    use crate::server::state::StateRef;
    use crate::transfer::messages::{JobDescription, TaskDescription};
    use crate::WrappedRcRefCell;
    use tako::worker::state::ServerLostPolicy;

    #[tokio::test]
    async fn test_log_failed_allocation_attempt_directory_fail() {
        let state = create_state(1000);

        let handler = Handler::new(
            WrappedRcRefCell::wrap(()),
            move |_, _| async move { anyhow::bail!("foo") },
            move |_, _| async move { Ok(Some(AllocationStatus::Queued)) },
            |_, _| async move { Ok(()) },
        );
        add_descriptor(&state, handler, QueueBuilder::default());

        autoalloc_tick(&state).await;

        let state = state.get();
        let state = state.get_autoalloc_state();
        let descriptor = state.get_descriptor(0).unwrap();
        matches!(
            descriptor.get_events()[0].event,
            AllocationEvent::QueueFail { .. }
        );
    }

    #[tokio::test]
    async fn test_log_failed_allocation_attempt_submission_fail() {
        let state = create_state(1000);

        let handler = Handler::new(
            WrappedRcRefCell::wrap(()),
            move |_, _| async move {
                Ok(AllocationSubmissionResult::new(
                    Err(anyhow::anyhow!("foo")),
                    Default::default(),
                ))
            },
            move |_, _| async move { Ok(Some(AllocationStatus::Queued)) },
            |_, _| async move { Ok(()) },
        );
        add_descriptor(&state, handler, QueueBuilder::default());

        autoalloc_tick(&state).await;

        let state = state.get();
        let state = state.get_autoalloc_state();
        let descriptor = state.get_descriptor(0).unwrap();
        matches!(
            descriptor.get_events()[0].event,
            AllocationEvent::QueueFail { .. }
        );
    }

    #[tokio::test]
    async fn test_fill_backlog() {
        let state = create_state(1000);

        let handler = always_queued_handler();
        add_descriptor(
            &state,
            handler,
            QueueBuilder::default().backlog(4).workers_per_alloc(2),
        );

        autoalloc_tick(&state).await;

        let allocations = get_allocations(&state, 0);
        assert_eq!(allocations.len(), 4);
        assert!(allocations.iter().all(|alloc| alloc.worker_count == 2));
    }

    #[tokio::test]
    async fn test_do_nothing_on_full_backlog() {
        let state = create_state(1000);

        let handler = always_queued_handler();
        add_descriptor(&state, handler, QueueBuilder::default().backlog(4));

        autoalloc_tick(&state).await;
        autoalloc_tick(&state).await;
        autoalloc_tick(&state).await;
        autoalloc_tick(&state).await;

        assert_eq!(get_allocations(&state, 0).len(), 4);
    }

    #[tokio::test]
    async fn test_keep_backlog_filled() {
        let state = create_state(1000);

        let mut queue = Map::<AllocationId, isize>::new();
        queue.insert("0".to_string(), 0); // run immediately
        queue.insert("1".to_string(), 2); // run after two checks
        queue.insert("2".to_string(), 3); // run after three checks

        let handler = Handler::new(
            WrappedRcRefCell::wrap((0, queue)),
            move |state, _| async move {
                let id_state = &mut state.get_mut().0;
                let id = *id_state;
                *id_state += 1;
                Ok(AllocationSubmissionResult::new(
                    Ok(id.to_string()),
                    Default::default(),
                ))
            },
            move |state, id| async move {
                let queue_state = &mut state.get_mut().1;
                let queue_time = *queue_state.get(&id).unwrap_or(&1000);
                queue_state.insert(id, queue_time - 1);

                let status = if queue_time <= 0 {
                    AllocationStatus::Running {
                        started_at: SystemTime::now(),
                    }
                } else {
                    AllocationStatus::Queued
                };
                Ok(Some(status))
            },
            |_, _| async move { Ok(()) },
        );
        add_descriptor(&state, handler, QueueBuilder::default().backlog(3));

        // schedule allocations
        autoalloc_tick(&state).await;
        check_allocation_count(get_allocations(&state, 0), 3, 0);

        // add new job to queue
        autoalloc_tick(&state).await;
        check_allocation_count(get_allocations(&state, 0), 3, 1);

        autoalloc_tick(&state).await;
        check_allocation_count(get_allocations(&state, 0), 3, 1);

        autoalloc_tick(&state).await;
        check_allocation_count(get_allocations(&state, 0), 3, 2);

        autoalloc_tick(&state).await;
        check_allocation_count(get_allocations(&state, 0), 3, 3);
    }

    #[tokio::test]
    async fn test_do_not_create_allocations_without_tasks() {
        let state = create_state(0);

        let handler = always_queued_handler();
        add_descriptor(&state, handler, QueueBuilder::default().backlog(3));

        autoalloc_tick(&state).await;
        assert_eq!(get_allocations(&state, 0).len(), 0);
    }

    #[tokio::test]
    async fn test_do_not_fill_backlog_when_tasks_run_out() {
        let state = create_state(5);

        let handler = always_queued_handler();
        add_descriptor(
            &state,
            handler,
            QueueBuilder::default().backlog(5).workers_per_alloc(2),
        );

        // 5 tasks, 3 * 2 workers -> last two allocations should be ignored
        autoalloc_tick(&state).await;
        assert_eq!(get_allocations(&state, 0).len(), 3);
    }

    #[tokio::test]
    async fn test_ignore_task_with_high_time_request() {
        let state = create_state(0);
        state
            .get_mut()
            .add_job(create_job(1, Duration::from_secs(60 * 60)));

        let handler = always_queued_handler();
        add_descriptor(
            &state,
            handler,
            QueueBuilder::default().timelimit(Duration::from_secs(60 * 30)),
        );

        // Allocations last for 30 minutes, but job requires 60 minutes
        // Nothing should be scheduled
        autoalloc_tick(&state).await;
        assert_eq!(get_allocations(&state, 0).len(), 0);
    }

    #[tokio::test]
    async fn test_respect_max_worker_count() {
        let state = create_state(100);

        let alloc_state = WrappedRcRefCell::wrap(AllocationState::default());
        let handler = stateful_handler(alloc_state.clone());

        add_descriptor(
            &state,
            handler,
            QueueBuilder::default().backlog(4).max_worker_count(Some(5)),
        );

        // Put 4 allocations into the queue.
        autoalloc_tick(&state).await;
        assert_eq!(get_allocations(&state, 0).len(), 4);

        // Start 2 allocations
        let now = SystemTime::now();
        {
            let alloc_state = &mut alloc_state.get_mut();
            let running = AllocationStatus::Running { started_at: now };
            alloc_state.set_status("0", running.clone());
            alloc_state.set_status("1", running);
        }
        // Create only one additional allocation
        autoalloc_tick(&state).await;
        assert_eq!(get_allocations(&state, 0).len(), 5);

        // Finish one allocation
        {
            let alloc_state = &mut alloc_state.get_mut();
            alloc_state.set_status(
                "0",
                AllocationStatus::Finished {
                    started_at: now,
                    finished_at: now,
                },
            );
        }

        // One worker was freed, create an additional allocation
        autoalloc_tick(&state).await;
        assert_eq!(get_allocations(&state, 0).len(), 6);
    }

    #[tokio::test]
    async fn test_max_worker_count_shorten_last_allocation() {
        let state = create_state(100);
        let handler = always_queued_handler();

        add_descriptor(
            &state,
            handler,
            QueueBuilder::default()
                .backlog(2)
                .workers_per_alloc(4)
                .max_worker_count(Some(6)),
        );

        autoalloc_tick(&state).await;
        let allocations = get_allocations(&state, 0);
        assert_eq!(allocations.len(), 2);
        assert_eq!(allocations[0].worker_count, 4);
        assert_eq!(allocations[1].worker_count, 2);
    }

    #[tokio::test]
    async fn test_delete_stale_directories_of_unsubmitted_allocations() {
        let state = create_state(100);

        let shared = WrappedRcRefCell::wrap(Vec::new());
        let handler = Handler::new(
            shared.clone(),
            move |state, _| async move {
                let mut state = state.get_mut();

                let tempdir = TempDir::new("hq").unwrap();
                let dir = tempdir.into_path();
                state.push(dir.clone());

                Ok(AllocationSubmissionResult::new(
                    Err(anyhow::anyhow!("failure")),
                    dir,
                ))
            },
            move |_, _| async move { Ok(Some(AllocationStatus::Queued)) },
            |_, _| async move { Ok(()) },
        );

        let max_kept = 4;
        add_descriptor(
            &state,
            handler,
            QueueBuilder::default().max_kept_dirs(max_kept),
        );

        // Gather failures
        for _ in 0..max_kept {
            autoalloc_tick(&state).await;
        }
        for directory in shared.get().iter() {
            assert!(directory.exists());
        }

        // Delete oldest directory
        autoalloc_tick(&state).await;
        assert!(!shared.get()[0].exists());
        for directory in &shared.get()[1..] {
            assert!(directory.exists());
        }

        // Delete second oldest directory
        autoalloc_tick(&state).await;
        assert!(!shared.get()[1].exists());
        for directory in &shared.get()[2..] {
            assert!(directory.exists());
        }
    }

    #[tokio::test]
    async fn test_delete_stale_directories_of_finished_allocations() {
        let state = create_state(100);

        let shared = WrappedRcRefCell::wrap(AllocationState::default());
        let handler = stateful_handler(shared.clone());

        let descriptor_id = add_descriptor(
            &state,
            handler,
            QueueBuilder::default().backlog(1).max_kept_dirs(0),
        );

        let check_dir = |allocation_id: &str, exists: bool| {
            let state = state.get();
            let state = state.get_autoalloc_state();
            let descriptor = state.get_descriptor(descriptor_id).unwrap();
            let allocation = descriptor.get_allocation(allocation_id).unwrap();
            assert_eq!(allocation.working_dir.exists(), exists);
        };

        // Spawn a single allocation
        autoalloc_tick(&state).await;

        // Check that queued job directory was not removed
        autoalloc_tick(&state).await;
        check_dir("0", true);

        // Start the allocation
        shared.get_mut().start("0");

        // Check that running job directory was not removed
        autoalloc_tick(&state).await;
        check_dir("0", true);

        // Finish the allocation
        shared.get_mut().finish("0");

        // Check that finished job directory was removed
        autoalloc_tick(&state).await;
        check_dir("0", false);

        // Start next allocation
        shared.get_mut().start("1");
        autoalloc_tick(&state).await;
        // Fail the allocation
        shared.get_mut().fail("1");

        // Check that failed job directory was removed
        autoalloc_tick(&state).await;
        check_dir("1", false);
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
            StatusFnFut: Future<Output = AutoAllocResult<Option<AllocationStatus>>>,
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
            StatusFnFut: Future<Output = AutoAllocResult<Option<AllocationStatus>>>,
            RemoveFn: 'static + Fn(WrappedRcRefCell<State>, AllocationId) -> RemoveFnFut,
            RemoveFnFut: Future<Output = AutoAllocResult<()>>,
        > QueueHandler for Handler<ScheduleFn, StatusFn, RemoveFn, State>
    {
        fn submit_allocation(
            &mut self,
            _descriptor_id: DescriptorId,
            _queue_info: &QueueInfo,
            worker_count: u64,
        ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationSubmissionResult>>>> {
            let schedule_fn = self.schedule_fn.clone();
            let custom_state = self.custom_state.clone();

            Box::pin(async move { (schedule_fn.get())(custom_state.clone(), worker_count).await })
        }

        fn get_allocation_status(
            &self,
            allocation: &Allocation,
        ) -> Pin<Box<dyn Future<Output = AutoAllocResult<Option<AllocationStatus>>>>> {
            let status_fn = self.status_fn.clone();
            let custom_state = self.custom_state.clone();
            let allocation_id = allocation.id.clone();

            Box::pin(async move { (status_fn.get())(custom_state.clone(), allocation_id).await })
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
    struct AllocationState {
        state: Map<AllocationId, AllocationStatus>,
        spawned_allocations: usize,
    }

    impl AllocationState {
        fn set_status(&mut self, allocation: &str, status: AllocationStatus) {
            self.state.insert(allocation.to_string(), status);
        }
        fn start(&mut self, allocation: &str) {
            self.set_status(
                allocation,
                AllocationStatus::Running {
                    started_at: SystemTime::now(),
                },
            );
        }
        fn finish(&mut self, allocation: &str) {
            self.set_status(
                allocation,
                AllocationStatus::Finished {
                    started_at: SystemTime::now(),
                    finished_at: SystemTime::now(),
                },
            );
        }
        fn fail(&mut self, allocation: &str) {
            self.set_status(
                allocation,
                AllocationStatus::Failed {
                    started_at: SystemTime::now(),
                    finished_at: SystemTime::now(),
                },
            );
        }
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
            move |_, _| async move { Ok(Some(AllocationStatus::Queued)) },
            |_, _| async move { Ok(()) },
        )
    }

    /// Handler that spawns allocations with sequentially increasing IDs (starting at *0*) and
    /// returns allocation statuses from [`AllocationState`].
    fn stateful_handler(state: WrappedRcRefCell<AllocationState>) -> Box<dyn QueueHandler> {
        Handler::new(
            state.clone(),
            move |state, _worker_count| async move {
                let id_state = &mut state.get_mut().spawned_allocations;
                let id = *id_state;
                *id_state += 1;

                let tempdir = TempDir::new("hq").unwrap();
                let dir = tempdir.into_path();

                Ok(AllocationSubmissionResult::new(Ok(id.to_string()), dir))
            },
            move |state, id| async move {
                let state = &mut state.get_mut().state;
                let status = state.get(&id).cloned().unwrap_or(AllocationStatus::Queued);
                Ok(Some(status))
            },
            |_, _| async move { Ok(()) },
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
        #[builder(default = "1")]
        max_kept_dirs: usize,
    }

    impl QueueBuilder {
        fn build(self) -> (QueueInfo, usize) {
            let Queue {
                backlog,
                workers_per_alloc,
                timelimit,
                max_worker_count,
                max_kept_dirs,
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
                max_kept_dirs,
            )
        }
    }

    // Helper functions
    fn add_descriptor(
        state_ref: &StateRef,
        handler: Box<dyn QueueHandler>,
        queue_builder: QueueBuilder,
    ) -> DescriptorId {
        let (queue_info, max_kept_directories) = queue_builder.build();
        let descriptor = QueueDescriptor::new(
            ManagerType::Pbs,
            queue_info,
            None,
            handler,
            max_kept_directories,
        );

        let mut state = state_ref.get_mut();
        let state = state.get_autoalloc_state_mut();
        let id = state.descriptors().count() as DescriptorId;
        state.add_descriptor(id, descriptor);
        id
    }

    fn create_state(waiting_tasks: u32) -> StateRef {
        let state = StateRef::new(Duration::from_millis(100), Default::default());
        if waiting_tasks > 0 {
            state
                .get_mut()
                .add_job(create_job(waiting_tasks, Duration::from_secs(0)));
        }
        state
    }

    fn get_allocations(state: &StateRef, descriptor: DescriptorId) -> Vec<Allocation> {
        let state = state.get();
        let state = state.get_autoalloc_state();
        let mut allocations: Vec<_> = state
            .get_descriptor(descriptor)
            .unwrap()
            .all_allocations()
            .cloned()
            .collect();
        allocations.sort_by(|a, b| a.id.cmp(&b.id));
        allocations
    }

    fn check_allocation_count(allocations: Vec<Allocation>, queued: usize, running: usize) {
        assert_eq!(
            queued,
            allocations
                .iter()
                .filter(|a| matches!(a.status, AllocationStatus::Queued))
                .count()
        );
        assert_eq!(
            running,
            allocations
                .iter()
                .filter(|a| matches!(a.status, AllocationStatus::Running { .. }))
                .count()
        );
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
                    pin: false,
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
}
