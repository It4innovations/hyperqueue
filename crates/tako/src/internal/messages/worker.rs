use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::hwstats::WorkerHwStateMessage;
use crate::internal::common::resources::{CpuId, GenericResourceAmount, GenericResourceIndex};
use crate::internal::common::Map;
use crate::internal::messages::common::TaskFailInfo;
use crate::task::SerializedTaskContext;
use crate::{InstanceId, Priority};
use crate::{TaskId, WorkerId};

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerRegistrationResponse {
    pub worker_id: WorkerId,
    pub worker_addresses: Map<WorkerId, String>,
    pub resource_names: Vec<String>,
    pub server_idle_timeout: Option<Duration>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskMsg {
    pub id: TaskId,

    pub instance_id: InstanceId,

    pub user_priority: Priority,
    pub scheduler_priority: Priority,

    pub resources: crate::internal::common::resources::ResourceRequest,
    pub time_limit: Option<Duration>,
    pub n_outputs: u32,

    pub node_list: Vec<WorkerId>,

    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskIdsMsg {
    pub ids: Vec<TaskId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NewWorkerMsg {
    pub worker_id: WorkerId,
    pub address: String,
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
    SetReservation(bool),
    Stop,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFinishedMsg {
    pub id: TaskId,
    pub size: u64,
    /*#[serde(with = "serde_bytes")]
    pub r#type: Vec<u8>,*/
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskFailedMsg {
    pub id: TaskId,
    pub info: TaskFailInfo,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskRunningMsg {
    pub id: TaskId,
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
pub enum GenericResourceAllocationValue {
    Indices(Vec<GenericResourceIndex>),
    Sum(GenericResourceAmount),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct GenericResourceAllocation {
    pub resource: String,
    pub value: GenericResourceAllocationValue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct TaskResourceAllocation {
    pub cpus: Vec<CpuId>,
    pub generic_allocations: Vec<GenericResourceAllocation>,
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
}

#[derive(Serialize, Deserialize, Debug)]
//#[serde(tag = "op")]
pub enum FromWorkerMessage {
    TaskFinished(TaskFinishedMsg),
    TaskFailed(TaskFailedMsg),
    TaskRunning(TaskRunningMsg),
    DataDownloaded(DataDownloadedMsg),
    StealResponse(StealResponseMsg),
    Overview(WorkerOverview),
    Heartbeat,
    Stop(WorkerStopReason),
}
