use futures::future::Either;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

use crate::messages::common::TaskFailInfo;
use crate::worker::state::{WorkerState, WorkerStateRef};
use crate::TaskId;

pub enum TaskResult {
    Finished,
    Canceled,
    Timeouted,
}

impl From<StopReason> for TaskResult {
    fn from(r: StopReason) -> Self {
        match r {
            StopReason::Cancel => TaskResult::Canceled,
            StopReason::Timeout => TaskResult::Timeouted,
        }
    }
}

pub enum StopReason {
    Cancel,
    Timeout,
}

pub struct TaskEnv {
    stop_sender: Option<Sender<StopReason>>,
}

impl TaskEnv {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        TaskEnv { stop_sender: None }
    }

    fn send_stop(&mut self, reason: StopReason) {
        if let Some(sender) = std::mem::take(&mut self.stop_sender) {
            assert!(sender.send(reason).is_ok());
        } else {
            log::debug!("Stopping a task in stopping process");
        }
    }

    pub fn cancel_task(&mut self) {
        self.send_stop(StopReason::Cancel);
    }

    pub fn start_task(
        &mut self,
        state: &mut WorkerState,
        state_ref: WorkerStateRef,
        task_id: TaskId,
    ) {
        let task_fut = {
            assert_eq!(state.get_task(task_id).n_outputs, 0);
            assert!(self.stop_sender.is_none());
            let (end_sender, end_receiver) = oneshot::channel();
            self.stop_sender = Some(end_sender);
            state
                .task_launcher
                .start_task(state_ref.clone(), task_id, end_receiver)
                .task_future
        };

        tokio::task::spawn_local(async move {
            let time_limit = {
                let state = state_ref.get();
                if let Some(task) = state.find_task(task_id) {
                    task.time_limit
                } else {
                    // Task were removed before spawn take place
                    return;
                }
            };
            let result = if let Some(duration) = time_limit {
                let sleep = tokio::time::sleep(duration);
                tokio::pin!(sleep);
                match futures::future::select(task_fut, sleep).await {
                    Either::Left((r, _)) => r,
                    Either::Right((_, task_fut)) => {
                        {
                            let mut state = state_ref.get_mut();
                            let task = state.get_task_mut(task_id);
                            log::debug!("Task {} timeouted", task.id);
                            task.task_env_mut().unwrap().send_stop(StopReason::Timeout)
                        }
                        task_fut.await
                    }
                }
            } else {
                task_fut.await
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
        });
    }
}
