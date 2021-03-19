use crate::worker::subworker::{SubworkerRef, choose_subworker};
use crate::worker::state::{WorkerState, WorkerStateRef};
use crate::worker::task::Task;
use crate::worker::task::TaskRef;
use crate::worker::data::{DataObject, LocalData};
use crate::TaskId;
use crate::common::data::SerializationType;
use bytes::Bytes;
use crate::messages::common::ProgramDefinition;
use crate::worker::launcher::launch_program_from_task;

pub enum TaskEnv {
    Subworker(SubworkerRef),
    Empty,
}

impl TaskEnv {
    pub fn create(state: &mut WorkerState, task: &Task) -> Option<Self> {
        if task.type_id == 0 {
            Some(TaskEnv::Empty)
        } else {
            let subworker_ref = choose_subworker(state, task).unwrap();
            Some(TaskEnv::Subworker(subworker_ref))
        }
    }

    #[inline]
    pub fn is_uploaded(&self, data_obj: &DataObject) -> bool {
        match self {
            TaskEnv::Subworker(subworker_ref) => data_obj.is_in_subworker(&subworker_ref),
            TaskEnv::Empty => return true,
        }
    }

    #[inline]
    pub fn get_subworker(&self) -> Option<&SubworkerRef> {
        match self {
            TaskEnv::Subworker(sw_ref) => Some(&sw_ref),
            TaskEnv::Empty => None
        }
    }

    pub fn send_data(&self, data_id: TaskId, data: Bytes, serializer: SerializationType) {
        match self {
            TaskEnv::Subworker(subworker_ref) => subworker_ref.get().send_data(data_id, data, serializer),
            TaskEnv::Empty => { /* Do nothing */ }
        }
    }

    pub fn start_task(&mut self, state: &WorkerState, task: &Task, task_ref: &TaskRef) {
        match self {
            TaskEnv::Subworker(subworker_ref) => {
                let mut sw = subworker_ref.get_mut();
                sw.running_task = Some(task_ref.clone());
                sw.send_start_task(task)
            },
            TaskEnv::Empty => {
                launch_program_from_task(state.self_ref(), task_ref.clone())
            }
        }
    }
}