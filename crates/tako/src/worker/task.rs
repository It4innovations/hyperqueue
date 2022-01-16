use crate::common::resources::ResourceAllocation;
use crate::common::stablemap::ExtractKey;
use crate::common::WrappedRcRefCell;
use crate::messages::worker::ComputeTaskMsg;
use crate::server::system::{DefaultTaskSystem, TaskSystem};
use crate::worker::data::DataObjectRef;
use crate::worker::taskenv::TaskEnv;
use crate::{InstanceId, Priority, TaskId};
use std::time::Duration;

pub enum TaskState {
    Waiting(u32),
    Running(TaskEnv, ResourceAllocation),
}

pub struct Task<System: TaskSystem = DefaultTaskSystem> {
    pub id: TaskId,
    pub state: TaskState,
    pub priority: (Priority, Priority),
    pub deps: Vec<DataObjectRef>,
    pub instance_id: InstanceId,

    pub resources: crate::common::resources::ResourceRequest,
    pub time_limit: Option<Duration>,
    pub n_outputs: u32,
    pub body: System::Body,
}

impl<System: TaskSystem> Task<System> {
    pub fn new(message: ComputeTaskMsg<System>) -> Self {
        Self {
            id: message.id,
            priority: (message.user_priority, message.scheduler_priority),
            state: TaskState::Waiting(0),
            deps: Default::default(),
            instance_id: message.instance_id,
            resources: message.resources,
            time_limit: message.time_limit,
            n_outputs: message.n_outputs,
            body: message.body,
        }
    }

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

    pub fn resource_allocation(&self) -> Option<&ResourceAllocation> {
        match &self.state {
            TaskState::Running(_, a) => Some(a),
            TaskState::Waiting(_) => None,
        }
    }

    pub fn task_env_mut(&mut self) -> Option<&mut TaskEnv> {
        match self.state {
            TaskState::Running(ref mut env, _) => Some(env),
            _ => None,
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

pub type TaskRef<System = DefaultTaskSystem> = WrappedRcRefCell<Task<System>>;

impl<System: TaskSystem> TaskRef<System> {
    pub fn new(message: ComputeTaskMsg<System>) -> Self {
        TaskRef::wrap(Task {
            id: message.id,
            priority: (message.user_priority, message.scheduler_priority),
            state: TaskState::Waiting(0),
            deps: Default::default(),
            instance_id: message.instance_id,
            resources: message.resources,
            time_limit: message.time_limit,
            n_outputs: message.n_outputs,
            body: message.body,
        })
    }
}

impl<System: TaskSystem> ExtractKey<TaskId> for Task<System> {
    #[inline]
    fn extract_key(&self) -> TaskId {
        self.id
    }
}
