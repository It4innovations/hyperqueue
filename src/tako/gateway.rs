use serde::{Deserialize, Serialize, Serializer};
use crate::{TaskId, TaskTypeId};
use crate::tako::common::{TaskFailInfo, SubworkerDefinition};


#[derive(Deserialize, Serialize, Debug)]
pub struct TaskDef {
    pub id: TaskId,

    pub type_id: TaskTypeId,

    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,

    #[serde(default)]
    pub keep: bool,

    #[serde(default)]
    pub observe: bool
}


#[derive(Deserialize, Serialize, Debug)]
pub struct NewTasksMessage {
    pub tasks: Vec<TaskDef>
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ObserveTasksMessage {
    pub tasks: Vec<TaskId>
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TaskInfoRequest {
    pub tasks: Vec<TaskId> // If empty, then all tasks if assumed
}


#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "op")]
pub enum FromGatewayMessage {
    NewTasks(NewTasksMessage),
    ObserveTasks(ObserveTasksMessage),
    RegisterSubworker(SubworkerDefinition),
    GetTaskInfo(TaskInfoRequest),
    ServerInfo,
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
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskUpdate {
    pub id: TaskId,
    pub state: TaskState,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskFailedMessage {
    pub id: TaskId,
    pub info: TaskFailInfo
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
    pub tasks: Vec<TaskInfo>
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum ToGatewayMessage {
    NewTasksResponse(NewTasksResponse),
    TaskUpdate(TaskUpdate),
    TaskFailed(TaskFailedMessage),
    TaskInfo(TasksInfoResponse),
    Error(ErrorResponse),
    ServerInfo(ServerInfo),
}