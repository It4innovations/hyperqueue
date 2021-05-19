use bytes::Bytes;
use tokio::sync::oneshot::Sender;

use crate::common::data::SerializationType;
use crate::common::resources::ResourceAllocation;
use crate::worker::data::DataObject;
use crate::worker::launcher::launch_program_from_task;
use crate::worker::state::WorkerState;
use crate::worker::subworker::{choose_subworker, SubworkerRef};
use crate::worker::task::Task;
use crate::worker::task::TaskRef;
use crate::TaskId;

pub enum TaskEnv {
    Subworker(SubworkerRef),
    Launcher(Option<Sender<()>>),
    Invalid,
}

impl TaskEnv {
    pub fn create(state: &mut WorkerState, task: &Task) -> Option<Self> {
        if task.type_id == 0 {
            Some(TaskEnv::Launcher(None))
        } else {
            let subworker_ref = choose_subworker(state, task).unwrap();
            Some(TaskEnv::Subworker(subworker_ref))
        }
    }

    #[inline]
    pub fn is_uploaded(&self, data_obj: &DataObject) -> bool {
        match self {
            TaskEnv::Subworker(subworker_ref) => data_obj.is_in_subworker(&subworker_ref),
            TaskEnv::Launcher(_) => return true,
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }

    #[inline]
    pub fn get_subworker(&self) -> Option<&SubworkerRef> {
        match self {
            TaskEnv::Subworker(sw_ref) => Some(&sw_ref),
            TaskEnv::Launcher(_) => None,
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
            TaskEnv::Launcher(_) => { /* Do nothing */ }
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
            TaskEnv::Launcher(Some(cancel_sender)) => {
                assert!(cancel_sender.send(()).is_ok());
            }
            TaskEnv::Launcher(None) => {
                panic!("Canceling uninitialized launcher")
            }
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }

    pub fn start_task(
        &mut self,
        state: &WorkerState,
        task: &Task,
        task_ref: &TaskRef,
        allocation: &ResourceAllocation,
    ) {
        match self {
            TaskEnv::Subworker(subworker_ref) => {
                let mut sw = subworker_ref.get_mut();
                sw.running_task = Some(task_ref.clone());
                sw.send_start_task(task)
            }
            TaskEnv::Launcher(ref mut cancel_sender) => {
                assert_eq!(task.n_outputs, 0);
                assert!(cancel_sender.is_none());
                let sender = launch_program_from_task(state.self_ref(), task_ref.clone());
                *cancel_sender = Some(sender);
            }
            TaskEnv::Invalid => {
                unreachable!()
            }
        }
    }
}
