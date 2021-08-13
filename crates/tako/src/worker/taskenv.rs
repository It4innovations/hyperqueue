use bytes::Bytes;
use futures::future::Either;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

use crate::common::data::SerializationType;
use crate::messages::common::TaskFailInfo;
use crate::worker::data::DataObject;
use crate::worker::state::WorkerState;
use crate::worker::subworker::{choose_subworker, SubworkerRef};
use crate::worker::task::Task;
use crate::worker::task::TaskRef;
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

pub enum TaskEnv {
    Subworker(SubworkerRef),
    Inner(Option<Sender<StopReason>>),
    Invalid,
}

impl TaskEnv {
    pub fn create(state: &mut WorkerState, task: &Task) -> Option<Self> {
        if task.configuration.type_id == 0 {
            Some(TaskEnv::Inner(None))
        } else {
            let subworker_ref = choose_subworker(state, task).unwrap();
            Some(TaskEnv::Subworker(subworker_ref))
        }
    }

    #[inline]
    pub fn is_uploaded(&self, data_obj: &DataObject) -> bool {
        match self {
            TaskEnv::Subworker(subworker_ref) => data_obj.is_in_subworker(subworker_ref),
            TaskEnv::Inner(_) => true,
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }

    #[inline]
    pub fn get_subworker(&self) -> Option<&SubworkerRef> {
        match self {
            TaskEnv::Subworker(sw_ref) => Some(sw_ref),
            TaskEnv::Inner(_) => None,
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }

    pub fn send_data(&self, data_id: TaskId, data: Bytes, serializer: SerializationType) {
        match self {
            TaskEnv::Subworker(subworker_ref) => {
                subworker_ref.get().send_data(data_id, data, serializer)
            }
            TaskEnv::Inner(_) => { /* Do nothing */ }
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }

    fn send_stop(&mut self, reason: StopReason) {
        match self {
            TaskEnv::Subworker(_) => {
                todo!()
            }
            TaskEnv::Inner(ref mut cancel_sender) => {
                if let Some(sender) = std::mem::take(cancel_sender) {
                    assert!(sender.send(reason).is_ok());
                } else {
                    log::debug!("Stopping a task in stopping process");
                }
            }
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }

    pub fn cancel_task(&mut self) {
        self.send_stop(StopReason::Cancel);
    }

    fn start_inner_task(
        &mut self,
        state: &WorkerState,
        task_ref: TaskRef,
        end_receiver: oneshot::Receiver<StopReason>,
    ) {
        let task_fut = (*state.inner_task_launcher)(&task_ref, end_receiver);
        let state_ref = state.self_ref();

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
                    log::debug!("Inner task failed id={}", task_ref.get().id);
                    state.finish_task_failed(task_ref, TaskFailInfo::from_string(e.to_string()));
                }
            }

            /*            tokio::select! {
                            biased;
            /*                _ = end_receiver => {
                                let task_id = task_ref.get().id;
                                log::debug!("Inner task cancelled id={}", task_id);
                            }*/
                        }*/
        });
    }

    pub fn start_task(&mut self, state: &WorkerState, task: &Task, task_ref: &TaskRef) {
        match self {
            TaskEnv::Subworker(subworker_ref) => {
                let mut sw = subworker_ref.get_mut();
                sw.running_task = Some(task_ref.clone());
                sw.send_start_task(task)
            }
            TaskEnv::Inner(ref mut cancel_sender) => {
                assert_eq!(task.configuration.n_outputs, 0);
                assert!(cancel_sender.is_none());
                let (sender, receiver) = oneshot::channel();
                *cancel_sender = Some(sender);
                self.start_inner_task(state, task_ref.clone(), receiver);
            }
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }
}
