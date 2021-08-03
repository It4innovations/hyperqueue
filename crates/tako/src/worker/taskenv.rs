use bytes::Bytes;
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

pub enum TaskEnv {
    Subworker(SubworkerRef),
    Inner(Option<Sender<()>>),
    Invalid,
}

impl TaskEnv {
    pub fn create(state: &mut WorkerState, task: &Task) -> Option<Self> {
        if task.type_id == 0 {
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

    pub fn cancel_task(&mut self) {
        match std::mem::replace(self, TaskEnv::Invalid) {
            TaskEnv::Subworker(_) => {
                todo!()
            }
            TaskEnv::Inner(Some(cancel_sender)) => {
                assert!(cancel_sender.send(()).is_ok());
            }
            TaskEnv::Inner(None) => {
                panic!("Canceling uninitialized launcher")
            }
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }

    fn start_inner_task(
        &mut self,
        state: &WorkerState,
        task_ref: TaskRef,
        end_receiver: oneshot::Receiver<()>,
    ) {
        let task_fut = (*state.inner_task_launcher)(&task_ref);
        let state_ref = state.self_ref();

        tokio::task::spawn_local(async move {
            if task_ref.get().is_removed() {
                // Task was canceled in between start of the task and this spawn_local
                return;
            }

            tokio::select! {
                biased;
                _ = end_receiver => {
                    let task_id = task_ref.get().id;
                    log::debug!("Inner task cancelled id={}", task_id);
                }
                r = task_fut => {
                    match r {
                        Ok(()) => {
                            let mut state = state_ref.get_mut();
                            let task_id = task_ref.get().id;
                            log::debug!("Inner task finished id={}", task_id);
                            state.finish_task(task_ref, 0);
                        }
                        Err(e) => {
                            log::debug!("Inner task failed id={}", task_ref.get().id);
                            let mut state = state_ref.get_mut();
                            state.finish_task_failed(task_ref, TaskFailInfo::from_string(e.to_string()));
                        }
                    }
                }
            }
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
                assert_eq!(task.n_outputs, 0);
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
