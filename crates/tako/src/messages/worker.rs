use serde::{Deserialize, Serialize};

use crate::common::resources::ResourceAllocation;
use crate::common::Map;
use crate::messages::common::{
    SubworkerDefinition, TaskConfiguration, TaskFailInfo, WorkerConfiguration,
};
use crate::messages::gateway::OverviewRequest;
use crate::worker::hwmonitor::WorkerHwState;
use crate::{InstanceId, Priority};
use crate::{TaskId, WorkerId};

#[derive(Serialize, Deserialize, Debug)]
pub enum ConnectionRegistration {
    Worker(RegisterWorker),
    Custom,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorker {
    pub configuration: WorkerConfiguration,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerRegistrationResponse {
    pub worker_id: WorkerId,
    pub worker_addresses: Map<WorkerId, String>,
    pub subworker_definitions: Vec<SubworkerDefinition>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeTaskMsg {
    pub id: TaskId,

    pub instance_id: InstanceId,

    pub dep_info: Vec<(TaskId, u64, Vec<WorkerId>)>,

    pub configuration: TaskConfiguration,

    pub user_priority: Priority,
    pub scheduler_priority: Priority,
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
//#[serde(tag = "op")]
pub enum ToWorkerMessage {
    ComputeTask(ComputeTaskMsg),
    DeleteData(TaskIdMsg),
    StealTasks(TaskIdsMsg),
    CancelTasks(TaskIdsMsg),
    NewWorker(NewWorkerMsg),
    RegisterSubworker(SubworkerDefinition),
    GetOverview(OverviewRequest),
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

#[derive(Deserialize, Serialize, Debug)]
pub struct WorkerHwStateMessage {
    pub state: WorkerHwState,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerOverview {
    pub id: WorkerId,
    pub running_tasks: Vec<(TaskId, ResourceAllocation)>,
    pub placed_data: Vec<TaskId>,
    pub hw_state: Option<WorkerHwStateMessage>,
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
}
