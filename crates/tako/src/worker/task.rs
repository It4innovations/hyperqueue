use crate::common::resources::{ResourceAllocation, ResourceRequest};
use crate::common::WrappedRcRefCell;
use crate::messages::worker::ComputeTaskMsg;
use crate::worker::data::DataObjectRef;
use crate::worker::taskenv::TaskEnv;
use crate::{OutputId, Priority, TaskId, TaskTypeId};

pub enum TaskState {
    Waiting(u32),
    Uploading(TaskEnv, u32, ResourceAllocation),
    Running(TaskEnv, ResourceAllocation),
    Removed,
}

pub struct Task {
    pub id: TaskId,
    pub type_id: TaskTypeId,
    pub state: TaskState,
    pub priority: (Priority, Priority),
    pub deps: Vec<DataObjectRef>,
    pub resources: ResourceRequest,
    pub n_outputs: OutputId,
    pub spec: Vec<u8>,
}

impl Task {
    #[inline]
    pub fn is_waiting(&self) -> bool {
        matches!(self.state, TaskState::Waiting(_))
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        matches!(self.state, TaskState::Waiting(0))
    }

    #[inline]
    pub fn is_running(&self) -> bool {
        matches!(self.state, TaskState::Running(_, _))
    }

    pub fn is_removed(&self) -> bool {
        matches!(self.state, TaskState::Removed)
    }

    pub fn resource_allocation(&self) -> Option<&ResourceAllocation> {
        match &self.state {
            TaskState::Uploading(_, _, a) | TaskState::Running(_, a) => Some(a),
            TaskState::Waiting(_) => None,
            TaskState::Removed => unreachable!(),
        }
    }

    pub fn get_waiting(&self) -> u32 {
        match self.state {
            TaskState::Waiting(x) => x,
            _ => 0,
        }
    }

    pub fn decrease_waiting_count(&mut self) -> bool {
        match &mut self.state {
            TaskState::Waiting(ref mut x) => {
                assert!(*x > 0);
                *x -= 1;
                *x == 0
            }
            _ => unreachable!(),
        }
    }

    pub fn increase_waiting_count(&mut self) {
        match &mut self.state {
            TaskState::Waiting(ref mut x) => {
                *x += 1;
            }
            _ => unreachable!(),
        }
    }
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {
    pub fn new(message: ComputeTaskMsg) -> Self {
        TaskRef::wrap(Task {
            id: message.id,
            type_id: message.type_id,
            n_outputs: message.n_outputs,
            spec: message.spec,
            priority: (message.user_priority, message.scheduler_priority),
            state: TaskState::Waiting(0),
            deps: Default::default(),
            resources: message.resources,
        })
    }
}
