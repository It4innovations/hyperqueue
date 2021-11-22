use futures::future::Either;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

use crate::messages::common::TaskFailInfo;
use crate::worker::state::WorkerState;
use crate::worker::task::Task;

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

    pub fn start_task(&mut self, state: &WorkerState, task: &Task) {
        assert_eq!(task.configuration.n_outputs, 0);
        assert!(self.stop_sender.is_none());
        let (end_sender, end_receiver) = oneshot::channel();
        self.stop_sender = Some(end_sender);

        let task_fut = (*state.task_launcher)(state, task.id, end_receiver);
        let state_ref = state.self_ref();
        let task_ref = state.get_task(task.id).clone();

        tokio::task::spawn_local(async move {
            let time_limit = {
                let task = task_ref.get();
                if task.is_removed() {
                    // Task was canceled in between start of the task and this spawn_local
                    return;
                }
                task.configuration.time_limit
            };
            let result = if let Some(duration) = time_limit {
                let sleep = tokio::time::sleep(duration);
                tokio::pin!(sleep);
                match futures::future::select(task_fut, sleep).await {
                    Either::Left((r, _)) => r,
                    Either::Right((_, task_fut)) => {
                        {
                            let mut task = task_ref.get_mut();
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
                    let task_id = task_ref.get().id;
                    log::debug!("Inner task finished id={}", task_id);
                    state.finish_task(task_ref, 0);
                }
                Ok(TaskResult::Canceled) => {
                    log::debug!("Inner task canceled id={}", task_ref.get().id);
                    state.finish_task_cancel(task_ref);
                }
                Ok(TaskResult::Timeouted) => {
                    log::debug!("Inner task timeouted id={}", task_ref.get().id);
                    state.finish_task_failed(
                        task_ref,
                        TaskFailInfo::from_string("Time limit reached".to_string()),
                    );
                }
                Err(e) => {
                    log::debug!("Inner task failed id={}, error={:?}", task_ref.get().id, e);
                    state.finish_task_failed(task_ref, TaskFailInfo::from_string(e.to_string()));
                }
            }
        });
    }
}
