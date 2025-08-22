use crate::datasrv::{DataObjectId, OutputId};
use crate::gateway::{EntryType, TaskDataFlags};
use crate::hwstats::WorkerHwStateMessage;
use crate::internal::common::resources::{ResourceAmount, ResourceIndex};
use crate::internal::messages::common::TaskFailInfo;
use crate::resources::ResourceFractions;
use crate::task::SerializedTaskContext;
use crate::{InstanceId, Priority};
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
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskSeparateData {
    /// Index into shared data stored in [ComputeTasksMsg].
    pub shared_index: usize,
    pub id: TaskId,
    pub instance_id: InstanceId,
    pub scheduler_priority: Priority,
    pub node_list: Vec<WorkerId>,
    pub data_deps: Vec<DataObjectId>,
    pub entry: Option<EntryType>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ComputeTaskSharedData {
    pub user_priority: Priority,
    pub resources: crate::internal::common::resources::ResourceRequestVariants,
    pub time_limit: Option<Duration>,
    pub data_flags: TaskDataFlags,
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
    StealTasks(TaskIdsMsg),
    CancelTasks(TaskIdsMsg),
    NewWorker(NewWorkerMsg),
    LostWorker(WorkerId),
    /// Override the internally set overview interval with a new duration
    /// if it is **disabled** on the worker.
    /// If the worker has already enabled overview interval, then this does nothing.
    SetOverviewIntervalOverride(Option<Duration>),
    RemoveDataObjects(SmallVec<[DataObjectId; 1]>),
    PlacementResponse(DataObjectId, Option<WorkerId>),
    Stop,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskOutput {
    pub id: OutputId,
    pub size: u64,
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
    pub size: u64,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct DataStorageStats {
    pub locally_uploaded_objects: u32,
    pub locally_uploaded_bytes: u64,
    pub locally_downloaded_objects: u32,
    pub locally_downloaded_bytes: u64,
    pub remotely_uploaded_objects: u32,
    pub remotely_uploaded_bytes: u64,
    pub remotely_downloaded_objects: u32,
    pub remotely_downloaded_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataNodeOverview {
    pub objects: Vec<DataObjectOverview>,
    pub stats: DataStorageStats,
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
    Overview(Box<WorkerOverview>),
    Heartbeat,
    Stop(WorkerStopReason),
    PlacementQuery(DataObjectId),
    NewPlacement(DataObjectId),
}
