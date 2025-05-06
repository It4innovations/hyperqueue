use crate::datasrv::{DataId, DataObjectId};
use crate::gateway::TaskDataFlags;
use crate::hwstats::WorkerHwStateMessage;
use crate::internal::common::resources::{ResourceAmount, ResourceIndex};
use crate::internal::messages::common::TaskFailInfo;
use crate::resources::ResourceFractions;
use crate::task::SerializedTaskContext;
use crate::{InstanceId, Priority};
use crate::{TaskId, WorkerId};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thin_vec::ThinVec;

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerRegistrationResponse {
    pub worker_id: WorkerId,
    pub resource_names: Vec<String>,
    pub other_workers: Vec<NewWorkerMsg>,
    pub server_idle_timeout: Option<Duration>,
    pub server_uid: String,
    /// Override worker overview interval, if the worker does not have it configured
    pub worker_overview_interval_override: Option<Duration>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskMsg {
    pub id: TaskId,

    pub instance_id: InstanceId,

    pub user_priority: Priority,
    pub scheduler_priority: Priority,

    pub resources: crate::internal::common::resources::ResourceRequestVariants,
    pub time_limit: Option<Duration>,
    pub node_list: Vec<WorkerId>,

    pub data_deps: Vec<DataObjectId>,
    pub data_flags: TaskDataFlags,

    #[serde(with = "serde_bytes")]
    pub body: Box<[u8]>,
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
    ComputeTask(ComputeTaskMsg),
    StealTasks(TaskIdsMsg),
    CancelTasks(TaskIdsMsg),
    NewWorker(NewWorkerMsg),
    LostWorker(WorkerId),
    SetReservation(bool),
    /// Override the internally set overview interval with a new duration
    /// if it is **disabled** on the worker.
    /// If the worker has already enabled overview interval, then this does nothing.
    SetOverviewIntervalOverride(Option<Duration>),
    RemoveDataObjects(Vec<DataObjectId>),
    Stop,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskOutput {
    pub id: DataId,
    pub size: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFinishedMsg {
    pub id: TaskId,
    pub outputs: Vec<TaskOutput>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFailedMsg {
    pub id: TaskId,
    pub info: TaskFailInfo,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskRunningMsg {
    pub id: TaskId,
    #[serde(with = "serde_bytes")]
    pub context: SerializedTaskContext,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DataDownloadedMsg {
    pub id: TaskId,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum StealResponse {
    Ok,
    NotHere,
    Running,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StealResponseMsg {
    pub responses: Vec<(TaskId, StealResponse)>,
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
pub struct DataObjectOverview {
    pub id: DataObjectId,
    pub size: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataNodeOverview {
    pub objects: Vec<DataObjectOverview>,
    pub total_downloaded_count: u32,
    pub total_uploaded_count: u32,
    pub total_downloaded_bytes: usize,
    pub total_uploaded_bytes: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerOverview {
    pub id: WorkerId,
    pub running_tasks: Vec<(TaskId, TaskResourceAllocation)>,
    pub hw_state: Option<WorkerHwStateMessage>,
    pub data_node: DataNodeOverview,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WorkerStopReason {
    IdleTimeout,
    TimeLimitReached,
    Interrupted,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FromWorkerMessage {
    TaskFinished(TaskFinishedMsg),
    TaskFailed(TaskFailedMsg),
    TaskRunning(TaskRunningMsg),
    StealResponse(StealResponseMsg),
    Overview(WorkerOverview),
    Heartbeat,
    Stop(WorkerStopReason),
    PlacementQuery(DataObjectId),
}
