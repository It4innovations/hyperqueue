use crate::gateway::EntryType;
use crate::hwstats::WorkerHwStateMessage;
use crate::internal::common::resources::map::ResourceRqMap;
use crate::internal::common::resources::{ResourceAmount, ResourceIndex, ResourceRqId};
use crate::internal::messages::common::TaskFailInfo;
use crate::resources::{ResourceFractions, ResourceRequestVariants};
use crate::task::SerializedTaskContext;
use crate::{InstanceId, Priority, ResourceVariantId};
use crate::{TaskId, WorkerId};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::rc::Rc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerRegistrationResponse {
    pub worker_id: WorkerId,
    pub resource_names: Vec<String>,
    pub other_workers: Vec<NewWorkerMsg>,
    pub server_idle_timeout: Option<Duration>,
    pub server_uid: String,
    /// Override worker overview interval, if the worker does not have it configured
    pub worker_overview_interval_override: Option<Duration>,
    pub resource_rq_map: ResourceRqMap,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskSeparateData {
    /// Index into shared data stored in [ComputeTasksMsg].
    pub shared_index: usize,
    pub id: TaskId,
    pub resource_rq_id: ResourceRqId,
    pub resource_rq_variant: Option<ResourceVariantId>,
    pub instance_id: InstanceId,
    pub priority: Priority,
    pub node_list: Vec<WorkerId>,
    pub entry: Option<EntryType>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ComputeTaskSharedData {
    pub time_limit: Option<Duration>,
    pub body: Rc<[u8]>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTasksMsg {
    pub tasks: Vec<ComputeTaskSeparateData>,
    /// Data that is between multiple instances of tasks from `self.tasks`.
    pub shared_data: Vec<ComputeTaskSharedData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskIdsMsg {
    pub ids: Vec<TaskId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerResourceCounts {
    pub n_resources: Vec<ResourceAmount>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NewWorkerMsg {
    pub worker_id: WorkerId,
    pub address: String,
    pub resources: WorkerResourceCounts,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskIdMsg {
    pub id: TaskId,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ToWorkerMessage {
    ComputeTasks(ComputeTasksMsg),
    RetractTasks(TaskIdsMsg),
    CancelTasks(TaskIdsMsg),
    NewWorker(NewWorkerMsg),
    LostWorker(WorkerId),
    /// Override the internally set overview interval with a new duration
    /// if it is **disabled** on the worker.
    /// If the worker has already enabled overview interval, then this does nothing.
    SetOverviewIntervalOverride(Option<Duration>),
    NewResourceRequest(ResourceRqId, ResourceRequestVariants),
    Stop,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskRunningMsg {
    pub task_id: TaskId,
    pub rv_id: ResourceVariantId,
    #[serde(with = "serde_bytes")]
    pub context: SerializedTaskContext,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RetractResponseMsg {
    pub responses: Vec<TaskId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ResourceAllocation {
    pub resource: String,
    pub indices: Vec<(ResourceIndex, ResourceFractions)>,
    pub amount: ResourceAmount,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct TaskResourceAllocation {
    pub resources: Vec<ResourceAllocation>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerOverview {
    pub id: WorkerId,
    pub running_tasks: Vec<(TaskId, TaskResourceAllocation)>,
    pub hw_state: Option<WorkerHwStateMessage>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerStopReason {
    IdleTimeout,
    TimeLimitReached,
    Interrupted,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerTaskUpdate {
    Finished {
        task_id: TaskId,
    },
    Failed {
        task_id: TaskId,
        info: TaskFailInfo,
    },
    Running(TaskRunningMsg),
    RunningPrefilled(TaskRunningMsg),
    RejectRequest {
        task_id: TaskId,
        rv_id: ResourceVariantId,
    },
    EnableRequest {
        resource_rq_id: ResourceRqId,
        rv_id: ResourceVariantId,
    },
}

pub type TaskUpdates = SmallVec<[WorkerTaskUpdate; 2]>;

#[derive(Serialize, Deserialize, Debug)]
pub enum FromWorkerMessage {
    TaskUpdate(TaskUpdates),
    RetractResponse(RetractResponseMsg),
    Overview(Box<WorkerOverview>),
    Heartbeat,
    Stop(WorkerStopReason),
    Notify(WorkerNotifyMessage),
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct WorkerNotifyMessage {
    pub task_id: TaskId,
    pub message: Box<[u8]>,
}
