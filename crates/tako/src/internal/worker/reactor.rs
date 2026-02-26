use crate::datasrv::DataObjectId;
use crate::internal::common::resources::Allocation;
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    ComputeTasksMsg, FromWorkerMessage, TaskFailedMsg, TaskFinishedMsg, TaskOutput, TaskRunningMsg,
    TaskUpdates, WorkerTaskUpdate,
};
use crate::internal::server::worker::Worker;
use crate::internal::worker::localcomm::{LocalCommState, Registration, Token};
use crate::internal::worker::state::{WorkerState, WorkerStateRef};
use crate::internal::worker::task::{Task, TaskState};
use crate::internal::worker::task_comm::RunningTaskComm;
use crate::launcher::{TaskBuildContext, TaskFuture, TaskLaunchData, TaskLauncher, TaskResult};
use crate::task::SerializedTaskContext;
use crate::{ResourceVariantId, TaskId};
use futures::future::Either;
use std::alloc::alloc;
use std::cell::RefCell;
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
        let task_state = TaskState::Waiting {
            waiting_data_objects: if task.data_deps.is_empty() {
                0
            } else {
                let mut waiting: u32 = 0;
                task.data_deps.iter().for_each(|data_id| {
                    if !state.data_storage.has_object(*data_id) {
                        waiting += 1;
                        state.download_object(*data_id, task.id, task.priority)
                    }
                });
                waiting
            },
        };
        // If we're handling the last task, steal the shared data instead of cloning it.
        // This optimization helps to avoid cloning the shared data unnecessarily if we
        // handle only a single task.
        let shared = if index == task_count - 1 {
            std::mem::take(&mut msg.shared_data[task.shared_index])
        } else {
            shared.clone()
        };
        let mut new_task = Task::new(task, shared, task_state);
        if new_task.is_ready() {
            if try_start_task(state, &mut new_task, &mut task_updates, remaining_time) {
                state.add_task(new_task);
            }
        }
    }
    if !task_updates.is_empty() {
        state
            .comm()
            .send_message_to_server(FromWorkerMessage::TaskUpdate(task_updates));
    }
}

pub fn try_start_task(
    state: &mut WorkerState,
    task: &mut Task,
    task_updates: &mut TaskUpdates,
    remaining_time: Option<Duration>,
) -> bool {
    let rq = state
        .resource_rq_map
        .get(task.resource_rq_id)
        .get(task.resource_rq_variant);

    if let Some(time) = remaining_time {
        if time < rq.min_time() {
            // HARD REJECT
            todo!()
        }
    }

    let Some(allocation) = state.allocator.try_allocate(&rq) else {
        // SOFT REJECT
        todo!()
    };

    match launch_task(state, &task, &allocation) {
        Ok((task_comm, task_context)) => {
            task_updates.push(WorkerTaskUpdate::TaskRunning(TaskRunningMsg {
                task_id: task.id,
                rv_id: task.resource_rq_variant,
                context: task_context,
            }));
            task.state = TaskState::Running {
                comm: task_comm,
                allocation,
                outputs: Default::default(),
            };
            true
        }
        Err(e) => {
            todo!();
            false
        }
    }
}

fn launch_task(
    state: &WorkerState,
    task: &Task,
    allocation: &Allocation,
) -> crate::Result<(RunningTaskComm, SerializedTaskContext)> {
    let task_id = task.id;
    log::debug!("Launching task={}", task_id);

    let (end_sender, end_receiver) = oneshot::channel();
    let task_comm = RunningTaskComm::new(end_sender);

    let token = state.lc_state.borrow_mut().register(Registration::Task {
        task_id,
        input_map: if task.need_data_layer() {
            task.data_deps.clone()
        } else {
            None
        },
    });
    match state.task_launcher.build_task(
        TaskBuildContext {
            task,
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
            tokio::task::spawn_local(handle_task_future(
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
        if let Some(task) = state.find_task(task_id) {
            task.time_limit
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
                    let task = state.get_task_mut(task_id);
                    log::debug!("Task {} timeouted", task.id);
                    // Send a message to the task future that it should terminate
                    // because it has been timeouted. Currently, we trust the
                    // implementation of the task and we don't further timeout
                    // it here.
                    task.task_comm_mut().unwrap().send_timeout_notification();
                }
                task_future.await
            }
        }
    } else {
        task_future.await
    };
    let mut state = state_ref.get_mut();
    let (allocation, outputs) = match state.remove_task(task_id).unwrap().state {
        TaskState::Running {
            allocation,
            outputs,
            comm: _,
        } => (allocation, outputs),
        _ => unreachable!(),
    };
    state.allocator.release_allocation(allocation);
    state.lc_state.borrow_mut().unregister_token(&token);
    let mut task_updates = TaskUpdates::new();
    match result {
        Ok(TaskResult::Finished) => {
            log::debug!("Inner task finished id={task_id}");
            task_updates.push(WorkerTaskUpdate::Finished(TaskFinishedMsg {
                task_id,
                outputs,
            }));
        }
        Ok(TaskResult::Canceled) => {
            log::debug!("Inner task canceled id={task_id}");
            drop_outputs(&mut state, task_id, outputs);
        }
        Ok(TaskResult::Timeouted) => {
            log::debug!("Inner task timeouted id={task_id}");
            drop_outputs(&mut state, task_id, outputs);
            task_updates.push(WorkerTaskUpdate::Failed(TaskFailedMsg {
                task_id,
                info: TaskFailInfo::from_string("Time limit reached".to_string()),
            }));
        }
        Err(e) => {
            log::debug!("Inner task failed id={task_id}, error={e:?}");
            drop_outputs(&mut state, task_id, outputs);
            task_updates.push(WorkerTaskUpdate::Failed(TaskFailedMsg {
                task_id,
                info: TaskFailInfo::from_string(e.to_string()),
            }));
        }
    }
    if !task_updates.is_empty() {
        state
            .comm()
            .send_message_to_server(FromWorkerMessage::TaskUpdate(task_updates));
    }
}

fn drop_outputs(state: &mut WorkerState, task_id: TaskId, outputs: Vec<TaskOutput>) {
    for output in outputs {
        state
            .data_storage
            .remove_object(DataObjectId::new(task_id, output.id));
    }
}
