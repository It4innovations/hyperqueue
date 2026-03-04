use crate::gateway::EntryType;
use crate::internal::common::resources::Allocation;
use crate::internal::common::stablemap::ExtractKey;
use crate::internal::messages::worker::{ComputeTaskSeparateData, ComputeTaskSharedData};
use crate::internal::worker::task_comm::RunningTaskComm;
use crate::resources::ResourceRqId;
use crate::{InstanceId, Priority, ResourceVariantId, TaskId, WorkerId};
use std::rc::Rc;
use std::time::Duration;

pub enum TaskState {
    Waiting,
    Running {
        comm: RunningTaskComm,
        allocation: Rc<Allocation>,
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
}

impl Task {
    pub fn new(task: ComputeTaskSeparateData, shared: ComputeTaskSharedData) -> Self {
        Self {
            state: TaskState::Waiting,
            id: task.id,
            priority: task.priority,
            instance_id: task.instance_id,
            resource_rq_id: task.resource_rq_id,
            resource_rq_variant: task.resource_rq_variant,
            time_limit: shared.time_limit,
            body: shared.body,
            entry: task.entry,
            node_list: task.node_list,
        }
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
}

impl ExtractKey<TaskId> for Task {
    #[inline]
    fn extract_key(&self) -> TaskId {
        self.id
    }
}
