use serde::{Deserialize, Serialize, Serializer};

use crate::messages::common::{TaskConfiguration, TaskFailInfo, WorkerConfiguration};
use crate::messages::worker::WorkerOverview;
use crate::{Priority, TaskId, WorkerId};

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskDef {
    pub id: TaskId,
    pub conf: TaskConfiguration,

    #[serde(default)]
    pub priority: Priority,

    #[serde(default)]
    pub keep: bool,

    #[serde(default)]
    pub observe: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct NewTasksMessage {
    pub tasks: Vec<TaskDef>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ObserveTasksMessage {
    pub tasks: Vec<TaskId>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskInfoRequest {
    pub tasks: Vec<TaskId>, // If empty, then all tasks are assumed
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CancelTasks {
    pub tasks: Vec<TaskId>, // If empty, then all tasks are assumed
}

#[derive(Deserialize, Serialize, Debug)]
pub struct StopWorkerRequest {
    pub worker_id: WorkerId,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct OverviewRequest {
    pub enable_hw_overview: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct GenericResourceNames {
    pub resource_names: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "op")]
pub enum FromGatewayMessage {
    NewTasks(NewTasksMessage),
    ObserveTasks(ObserveTasksMessage),
    CancelTasks(CancelTasks),
    GetTaskInfo(TaskInfoRequest),
    ServerInfo,
    GetOverview(OverviewRequest),
    StopWorker(StopWorkerRequest),

    // Register names in the request (may be empty) and return ALL names
    // in message GenericResourceNames
    GetGenericResourceNames(GenericResourceNames),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NewTasksResponse {
    pub n_waiting_for_workers: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorResponse {
    pub message: String,
}

#[derive(Deserialize, Debug)]
pub enum TaskState {
    Invalid,
    Waiting,
    Running(WorkerId),
    Finished,
}

impl Serialize for TaskState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            TaskState::Invalid => "Invalid",
            TaskState::Waiting => "Waiting",
            TaskState::Finished => "Finished",
            TaskState::Running(_) => "Running",
        })
    }
}

/* User can receive this updates when task is registered with "observe flag"
  Note: Error state is NOT there, it is sent separately as TaskFail,
  because task fail is received even without "observe" flag.
*/
#[derive(Serialize, Deserialize, Debug)]
pub struct TaskUpdate {
    pub id: TaskId,
    pub state: TaskState,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskFailedMessage {
    pub id: TaskId,
    pub cancelled_tasks: Vec<TaskId>,
    pub info: TaskFailInfo,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerInfo {
    pub worker_listen_port: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskInfo {
    pub id: TaskId,
    pub state: TaskState,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TasksInfoResponse {
    pub tasks: Vec<TaskInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CollectedOverview {
    pub worker_overviews: Vec<WorkerOverview>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CancelTasksResponse {
    // Tasks that was waiting, assigned or running. Such tasks were removed from server
    // and force stop command was send to workers.
    // This also contains a ids of waiting tasks that were recursively canceled
    // (recursive consumers of tasks in cancel request)
    pub cancelled_tasks: Vec<TaskId>,

    // Tasks that was already finished when cancel request was received
    // if there was an keep flag, it was removed
    pub already_finished: Vec<TaskId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NewWorkerMessage {
    pub worker_id: WorkerId,
    pub configuration: WorkerConfiguration,
}

#[derive(Deserialize, Debug)]
pub enum LostWorkerReason {
    Stopped,
    ConnectionLost,
    HeartbeatLost,
    IdleTimeout,
}

impl Serialize for LostWorkerReason {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            LostWorkerReason::Stopped => "Stopped",
            LostWorkerReason::ConnectionLost => "ConnectionLost",
            LostWorkerReason::HeartbeatLost => "HeartbeatLost",
            LostWorkerReason::IdleTimeout => "IdleTimeout",
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LostWorkerMessage {
    pub worker_id: WorkerId,
    pub running_tasks: Vec<TaskId>,
    pub reason: LostWorkerReason,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct GenericResourceNamesResponse {
    pub resource_names: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum ToGatewayMessage {
    NewTasksResponse(NewTasksResponse),
    CancelTasksResponse(CancelTasksResponse),
    TaskUpdate(TaskUpdate),
    TaskFailed(TaskFailedMessage),
    TaskInfo(TasksInfoResponse),
    Error(ErrorResponse),
    ServerInfo(ServerInfo),
    Overview(CollectedOverview),
    NewWorker(NewWorkerMessage),
    LostWorker(LostWorkerMessage),
    WorkerStopped,
    GenericResourceNames(GenericResourceNames),
}
