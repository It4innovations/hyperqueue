use crate::internal::common::resources::Allocation;
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    ComputeTasksMsg, FromWorkerMessage, TaskRunningMsg, TaskUpdates, WorkerTaskUpdate,
};
use crate::internal::worker::localcomm::{Registration, Token};
use crate::internal::worker::state::{WorkerState, WorkerStateRef};
use crate::internal::worker::task::{RunningTask, Task};
use crate::internal::worker::task_comm::RunningTaskComm;
use crate::launcher::{TaskBuildContext, TaskFuture, TaskLaunchData, TaskResult};
use crate::resources::ResourceRqId;
use crate::task::SerializedTaskContext;
use crate::{ResourceVariantId, TaskId};
use futures::future::Either;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::oneshot;

pub(crate) fn compute_tasks(state: &mut WorkerState, mut msg: ComputeTasksMsg) {
    let task_count = msg.tasks.len();
    let mut task_updates = TaskUpdates::new();
    let remaining_time = state.remaining_time();
    for (index, task) in msg.tasks.into_iter().enumerate() {
        let shared = &msg.shared_data[task.shared_index];
        log::debug!("Task assigned: {}", task.id);
        // If we're handling the last task, steal the shared data instead of cloning it.
        // This optimization helps to avoid cloning the shared data unnecessarily if we
        // handle only a single task.
        let shared = if index == task_count - 1 {
            std::mem::take(&mut msg.shared_data[task.shared_index])
        } else {
            shared.clone()
        };
        let (new_task, rv_id) = Task::new(task, shared);
        if let Some(rv_id) = rv_id {
            try_alloc_and_start_task(state, new_task, rv_id, remaining_time, &mut task_updates);
        } else {
            state
                .prefilled_tasks
                .entry(new_task.resource_rq_id)
                .or_default()
                .push(new_task);
        }
    }
    if !task_updates.is_empty() {
        state
            .comm()
            .send_message_to_server(FromWorkerMessage::TaskUpdate(task_updates));
    }
}

fn try_alloc_and_start_task(
    state: &mut WorkerState,
    task: Task,
    rv_id: ResourceVariantId,
    _remaining_time: Option<Duration>,
    task_updates: &mut TaskUpdates,
) {
    let rq = state.resource_rq_map.get(task.resource_rq_id).get(rv_id);
    let Some(allocation) = state.allocator.try_allocate(rq) else {
        // Soft reject, we remember rejection as we unblock in the future
        state.blocked_requests.insert((task.resource_rq_id, rv_id));
        task_updates.push(WorkerTaskUpdate::RejectRequest {
            task_id: task.id,
            rv_id,
        });
        return;
    };
    let resource_rq_id = task.resource_rq_id;
    let Some(allocation) = try_start_task(state, task, rv_id, false, allocation, task_updates)
    else {
        return;
    };
    prefill_loop(state, resource_rq_id, rv_id, allocation, task_updates);
}

fn prefill_loop(
    state: &mut WorkerState,
    resource_rq_id: ResourceRqId,
    rv_id: ResourceVariantId,
    mut allocation: Rc<Allocation>,
    task_updates: &mut TaskUpdates,
) -> bool {
    while let Some(task) = state
        .prefilled_tasks
        .get_mut(&resource_rq_id)
        .and_then(|ts| ts.pop())
    {
        let Some(a) = try_start_task(state, task, rv_id, true, allocation, task_updates) else {
            return true;
        };
        allocation = a;
    }
    state.allocator.release_allocation(allocation);
    false
}

fn try_start_task(
    state: &mut WorkerState,
    task: Task,
    rv_id: ResourceVariantId,
    prefilled: bool,
    allocation: Rc<Allocation>,
    task_updates: &mut TaskUpdates,
) -> Option<Rc<Allocation>> {
    let rq = state.resource_rq_map.get(task.resource_rq_id).get(rv_id);

    if let Some(time) = state.remaining_time()
        && time < rq.min_time()
    {
        // Hard reject, we never unblock this rejection so we do not need to update blocked requests
        task_updates.push(WorkerTaskUpdate::RejectRequest {
            task_id: task.id,
            rv_id,
        });
        return Some(allocation);
    }

    // let Some(allocation) = state.allocator.try_allocate(&rq) else {
    //     // Soft reject, we remember rejection as we unblock in the future
    //     state
    //         .blocked_requests
    //         .insert((task.resource_rq_id, resource_rq_variant));
    //     task_updates.push(WorkerTaskUpdate::RejectRequest {
    //         task_id: task.id,
    //         resource_rq_variant,
    //     });
    //     return Some(allocation);
    // };

    match launch_task(state, &task, rv_id, &allocation) {
        Ok((task_comm, task_context)) => {
            let msg = TaskRunningMsg {
                task_id: task.id,
                rv_id,
                context: task_context,
            };
            task_updates.push(if prefilled {
                WorkerTaskUpdate::RunningPrefilled(msg)
            } else {
                WorkerTaskUpdate::Running(msg)
            });
            let rtask = RunningTask::new(task, rv_id, task_comm, allocation);
            state.running_tasks.insert(rtask);
            None
        }
        Err(e) => {
            task_updates.push(WorkerTaskUpdate::Failed {
                task_id: task.id,
                info: TaskFailInfo::from_string(e.to_string()),
            });
            Some(allocation)
        }
    }
}

fn launch_task(
    state: &mut WorkerState,
    task: &Task,
    rv_id: ResourceVariantId,
    allocation: &Allocation,
) -> crate::Result<(RunningTaskComm, SerializedTaskContext)> {
    let task_id = task.id;
    log::debug!("Launching task={}", task_id);

    let (end_sender, end_receiver) = oneshot::channel();
    let task_comm = RunningTaskComm::new(end_sender);

    let token = state
        .lc_state
        .borrow_mut()
        .register(Registration::Task { task_id });
    match state.task_launcher.build_task(
        TaskBuildContext {
            task,
            rv_id,
            allocation,
            state,
            token: token.clone(),
        },
        end_receiver,
    ) {
        Ok(task_launch_data) => {
            let TaskLaunchData {
                task_future,
                task_context,
            } = task_launch_data;
            let state_ref = state.state_ref();
            state.comm().spawn_task(handle_task_future(
                task_future,
                state_ref,
                task_id,
                token.clone(),
            ));
            Ok((task_comm, task_context))
        }
        Err(error) => {
            log::debug!("Task initialization failed id={task_id}, error={error:?}");
            state.lc_state.borrow_mut().unregister_token(&token);
            Err(error)
            //state.finish_task_failed(task_id, TaskFailInfo::from_string(error.to_string()));
        }
    }
}

/// Polls the task future and makes sure that various situations
/// (like cancellation, errors, timeout) are handled correctly.
async fn handle_task_future(
    task_future: TaskFuture,
    state_ref: WorkerStateRef,
    task_id: TaskId,
    token: Token,
) {
    let time_limit = {
        let state = state_ref.get();
        if let Some(rtask) = state.find_running_task(task_id) {
            rtask.task.time_limit
        } else {
            state.lc_state.borrow_mut().unregister_token(&token);
            // Task was removed before spawn took place
            return;
        }
    };

    let result = if let Some(duration) = time_limit {
        let sleep = tokio::time::sleep(duration);
        tokio::pin!(sleep);
        match futures::future::select(task_future, sleep).await {
            Either::Left((r, _)) => r,
            Either::Right((_, task_future)) => {
                {
                    let mut state = state_ref.get_mut();
                    let task = state.get_running_task_mut(task_id);
                    log::debug!("Task {} timeouted", task_id);
                    // Send a message to the task future that it should terminate
                    // because it has been timeouted. Currently, we trust the
                    // implementation of the task and we don't further timeout
                    // it here.
                    task.send_timeout_notification();
                }
                task_future.await
            }
        }
    } else {
        task_future.await
    };
    let mut state = state_ref.get_mut();
    let rtask = state.remove_running_task(task_id).unwrap();
    let resource_rq_id = rtask.task.resource_rq_id;
    state.running_tasks.remove(&task_id);
    state.lc_state.borrow_mut().unregister_token(&token);
    let mut task_updates = TaskUpdates::new();
    match result {
        Ok(TaskResult::Finished) => {
            log::debug!("Inner task finished id={task_id}");
            task_updates.push(WorkerTaskUpdate::Finished { task_id });
        }
        Ok(TaskResult::Canceled) => {
            log::debug!("Inner task canceled id={task_id}");
        }
        Ok(TaskResult::Timeouted) => {
            log::debug!("Inner task timeouted id={task_id}");
            task_updates.push(WorkerTaskUpdate::Failed {
                task_id,
                info: TaskFailInfo::from_string("Time limit reached".to_string()),
            });
        }
        Err(e) => {
            log::debug!("Inner task failed id={task_id}, error={e:?}");
            task_updates.push(WorkerTaskUpdate::Failed {
                task_id,
                info: TaskFailInfo::from_string(e.to_string()),
            });
        }
    }

    let allocation_used = prefill_loop(
        &mut state,
        resource_rq_id,
        rtask.rv_id,
        rtask.allocation,
        &mut task_updates,
    );

    if !allocation_used && !state.blocked_requests.is_empty() {
        let mut unblocked = Vec::new();

        for (rq_id, rv_id) in &state.blocked_requests {
            let rq = state.resource_rq_map.get(*rq_id).get(*rv_id);
            if state.allocator.is_enabled(rq) {
                unblocked.push((*rq_id, *rv_id));
            }
        }
        for (rq_id, rv_id) in unblocked {
            task_updates.push(WorkerTaskUpdate::EnableRequest {
                resource_rq_id: rq_id,
                rv_id,
            });
            state.blocked_requests.remove(&(rq_id, rv_id));
        }
    }

    if !task_updates.is_empty() {
        state
            .comm()
            .send_message_to_server(FromWorkerMessage::TaskUpdate(task_updates));
    }
}
