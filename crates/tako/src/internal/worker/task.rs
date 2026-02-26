use crate::datasrv::DataObjectId;
use crate::gateway::{EntryType, TaskDataFlags};
use crate::internal::common::resources::Allocation;
use crate::internal::common::stablemap::ExtractKey;
use crate::internal::messages::worker::{
    ComputeTaskSeparateData, ComputeTaskSharedData, TaskOutput,
};
use crate::internal::worker::task_comm::RunningTaskComm;
use crate::resources::ResourceRqId;
use crate::{InstanceId, Priority, ResourceVariantId, TaskId, WorkerId};
use std::rc::Rc;
use std::time::Duration;

pub enum TaskState {
    Waiting {
        waiting_data_objects: u32,
    },
    Running {
        comm: RunningTaskComm,
        allocation: Rc<Allocation>,
        outputs: Vec<TaskOutput>,
    },
}

pub struct Task {
    pub id: TaskId,
    pub state: TaskState,
    pub priority: Priority,
    pub instance_id: InstanceId,

    pub resource_rq_id: ResourceRqId,
    pub resource_rq_variant: ResourceVariantId,
    pub time_limit: Option<Duration>,
    pub body: Rc<[u8]>,
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
            priority: task.priority,
            instance_id: task.instance_id,
            resource_rq_id: task.resource_rq_id,
            resource_rq_variant: task.resource_rq_variant,
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
        matches!(self.state, TaskState::Waiting { .. })
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        matches!(
            self.state,
            TaskState::Waiting {
                waiting_data_objects: 0
            }
        )
    }

    #[inline]
    pub fn is_running(&self) -> bool {
        matches!(self.state, TaskState::Running { .. })
    }

    pub fn resource_allocation(&self) -> Option<&Allocation> {
        match &self.state {
            TaskState::Running { allocation, .. } => Some(allocation),
            TaskState::Waiting { .. } => None,
        }
    }

    pub fn task_comm_mut(&mut self) -> Option<&mut RunningTaskComm> {
        match self.state {
            TaskState::Running { ref mut comm, .. } => Some(comm),
            _ => None,
        }
    }

    pub fn get_waiting(&self) -> u32 {
        match self.state {
            TaskState::Waiting {
                waiting_data_objects,
            } => waiting_data_objects,
            _ => 0,
        }
    }

    pub fn decrease_waiting_count(&mut self) -> bool {
        match &mut self.state {
            TaskState::Waiting {
                waiting_data_objects,
            } => {
                assert!(*waiting_data_objects > 0);
                *waiting_data_objects -= 1;
                *waiting_data_objects == 0
            }
            _ => unreachable!(),
        }
    }

    pub fn increase_waiting_count(&mut self) {
        match &mut self.state {
            TaskState::Waiting {
                waiting_data_objects,
            } => {
                *waiting_data_objects += 1;
            }
            _ => unreachable!(),
        }
    }

    pub fn need_data_layer(&self) -> bool {
        self.data_flags.contains(TaskDataFlags::ENABLE_DATA_LAYER)
    }

    pub fn add_output(&mut self, task_output: TaskOutput) {
        match &mut self.state {
            TaskState::Waiting { .. } => {
                panic!("Task is not in valid state");
            }
            TaskState::Running { outputs, .. } => outputs.push(task_output),
        }
    }
}

impl ExtractKey<TaskId> for Task {
    #[inline]
    fn extract_key(&self) -> TaskId {
        self.id
    }
}
