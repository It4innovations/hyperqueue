use crate::common::resources::ResourceAllocation;
use crate::messages::common::TaskFailInfo;
use crate::messages::worker::{FromWorkerMessage, TaskRunningMsg};
use crate::worker::launcher::{TaskFuture, TaskLaunchData};
use crate::worker::state::{WorkerState, WorkerStateRef};
use crate::worker::taskenv::{StopReason, TaskEnv, TaskResult};
use crate::TaskId;
use futures::future::Either;
use tokio::sync::oneshot;

pub fn run_task(
    state: &mut WorkerState,
    state_ref: &WorkerStateRef,
    task_id: TaskId,
    allocation: ResourceAllocation,
) {
    log::debug!("Task={} assigned", task_id);

    assert_eq!(state.get_task(task_id).n_outputs, 0);
    let (end_sender, end_receiver) = oneshot::channel();
    let task_env = TaskEnv::new(end_sender);

    state.start_task(task_id, task_env, allocation);

    match state.task_launcher.build_task(state, task_id, end_receiver) {
        Ok(task_launch_data) => {
            let TaskLaunchData {
                task_future,
                running_data: _,
            } = task_launch_data;

            state.send_message_to_server(FromWorkerMessage::TaskRunning(TaskRunningMsg {
                id: task_id,
            }));

            tokio::task::spawn_local(execute_task(task_future, state_ref.clone(), task_id));
        }
        Err(error) => {
            log::debug!(
                "Task initialization failed id={}, error={:?}",
                task_id,
                error
            );
            state.finish_task_failed(task_id, TaskFailInfo::from_string(error.to_string()));
        }
    };
}

async fn execute_task(task_future: TaskFuture, state_ref: WorkerStateRef, task_id: TaskId) {
    let time_limit = {
        let state = state_ref.get();
        if let Some(task) = state.find_task(task_id) {
            task.time_limit
        } else {
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
                    task.task_env_mut().unwrap().send_stop(StopReason::Timeout)
                }
                task_future.await
            }
        }
    } else {
        task_future.await
    };
    let mut state = state_ref.get_mut();
    match result {
        Ok(TaskResult::Finished) => {
            log::debug!("Inner task finished id={}", task_id);
            state.finish_task(task_id, 0);
        }
        Ok(TaskResult::Canceled) => {
            log::debug!("Inner task canceled id={}", task_id);
            state.finish_task_cancel(task_id);
        }
        Ok(TaskResult::Timeouted) => {
            log::debug!("Inner task timeouted id={}", task_id);
            state.finish_task_failed(
                task_id,
                TaskFailInfo::from_string("Time limit reached".to_string()),
            );
        }
        Err(e) => {
            log::debug!("Inner task failed id={}, error={:?}", task_id, e);
            state.finish_task_failed(task_id, TaskFailInfo::from_string(e.to_string()));
        }
    }
}
