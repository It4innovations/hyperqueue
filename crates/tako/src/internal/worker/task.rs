use crate::datasrv::DataObjectId;
use crate::gateway::{EntryType, TaskDataFlags};
use crate::internal::common::resources::Allocation;
use crate::internal::common::stablemap::ExtractKey;
use crate::internal::messages::worker::{
    ComputeTaskSeparateData, ComputeTaskSharedData, TaskOutput,
};
use crate::internal::worker::task_comm::RunningTaskComm;
use crate::{InstanceId, Priority, TaskId, WorkerId};
use std::rc::Rc;
use std::time::Duration;

pub struct RunningState {
    pub comm: RunningTaskComm,
    pub allocation: Rc<Allocation>,
    pub outputs: Vec<TaskOutput>,
}

pub enum TaskState {
    Waiting(u32),
    Running(RunningState),
}

pub struct Task {
    pub id: TaskId,
    pub state: TaskState,
    pub priority: (Priority, Priority),
    pub instance_id: InstanceId,

    pub resources: crate::internal::common::resources::ResourceRequestVariants,
    pub time_limit: Option<Duration>,
    pub body: Box<[u8]>,
    pub entry: Option<EntryType>,
    pub node_list: Vec<WorkerId>, // Filled in multi-node tasks; otherwise empty

    pub data_deps: Option<Rc<Vec<DataObjectId>>>,
    pub data_flags: TaskDataFlags,
}

impl Task {
    pub fn new(
        task: ComputeTaskSeparateData,
        shared: ComputeTaskSharedData,
        task_state: TaskState,
    ) -> Self {
        Self {
            state: task_state,
            id: task.id,
            priority: (shared.user_priority, task.scheduler_priority),
            instance_id: task.instance_id,
            resources: shared.resources,
            time_limit: shared.time_limit,
            body: shared.body,
            entry: task.entry,
            node_list: task.node_list,
            data_deps: (!task.data_deps.is_empty()).then(|| Rc::new(task.data_deps)),
            data_flags: shared.data_flags,
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
        matches!(self.state, TaskState::Running(_))
    }

    pub fn resource_allocation(&self) -> Option<&Allocation> {
        match &self.state {
            TaskState::Running(s) => Some(&s.allocation),
            TaskState::Waiting(_) => None,
        }
    }

    pub fn task_comm_mut(&mut self) -> Option<&mut RunningTaskComm> {
        match self.state {
            TaskState::Running(ref mut s) => Some(&mut s.comm),
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
            TaskState::Waiting(x) => {
                assert!(*x > 0);
                *x -= 1;
                *x == 0
            }
            _ => unreachable!(),
        }
    }

    pub fn increase_waiting_count(&mut self) {
        match &mut self.state {
            TaskState::Waiting(x) => {
                *x += 1;
            }
            _ => unreachable!(),
        }
    }

    pub fn need_data_layer(&self) -> bool {
        self.data_flags.contains(TaskDataFlags::ENABLE_DATA_LAYER)
    }

    pub fn add_output(&mut self, task_output: TaskOutput) {
        match &mut self.state {
            TaskState::Waiting(_) => {
                panic!("Task is not in valid state");
            }
            TaskState::Running(s) => s.outputs.push(task_output),
        }
    }
}

impl ExtractKey<TaskId> for Task {
    #[inline]
    fn extract_key(&self) -> TaskId {
        self.id
    }
}
