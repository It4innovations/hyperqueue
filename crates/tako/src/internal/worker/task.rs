use crate::gateway::EntryType;
use crate::internal::common::resources::Allocation;
use crate::internal::common::stablemap::ExtractKey;
use crate::internal::messages::worker::{ComputeTaskSeparateData, ComputeTaskSharedData};
use crate::internal::worker::task_comm::RunningTaskComm;
use crate::resources::ResourceRqId;
use crate::{InstanceId, Priority, ResourceVariantId, TaskId, WorkerId};
use std::rc::Rc;
use std::time::Duration;

pub struct RunningTask {
    pub task: Task,
    comm: RunningTaskComm,
    pub allocation: Rc<Allocation>,
    pub rv_id: ResourceVariantId,
}

pub struct Task {
    pub id: TaskId,
    pub priority: Priority,
    pub instance_id: InstanceId,

    pub resource_rq_id: ResourceRqId,
    pub time_limit: Option<Duration>,
    pub body: Rc<[u8]>,
    pub entry: Option<EntryType>,
    pub node_list: Vec<WorkerId>, // Filled in multi-node tasks; otherwise empty
}

impl Task {
    pub fn new(
        task: ComputeTaskSeparateData,
        shared: ComputeTaskSharedData,
    ) -> (Self, Option<ResourceVariantId>) {
        (
            Self {
                id: task.id,
                priority: task.priority,
                instance_id: task.instance_id,
                resource_rq_id: task.resource_rq_id,
                time_limit: shared.time_limit,
                body: shared.body,
                entry: task.entry,
                node_list: task.node_list,
            },
            task.resource_rq_variant,
        )
    }
}

impl ExtractKey<TaskId> for Task {
    #[inline]
    fn extract_key(&self) -> TaskId {
        self.id
    }
}

impl ExtractKey<TaskId> for RunningTask {
    #[inline]
    fn extract_key(&self) -> TaskId {
        self.task.id
    }
}

impl RunningTask {
    pub fn new(
        task: Task,
        rv_id: ResourceVariantId,
        comm: RunningTaskComm,
        allocation: Rc<Allocation>,
    ) -> Self {
        RunningTask {
            task,
            comm,
            allocation,
            rv_id,
        }
    }

    pub fn cancel(&mut self) {
        self.comm.send_cancel_notification();
    }

    pub fn send_timeout_notification(&mut self) {
        self.comm.send_timeout_notification();
    }
}
