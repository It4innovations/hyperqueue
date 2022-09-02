use serde::{Deserialize, Serialize, Serializer};

use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::WorkerOverview;
use crate::internal::worker::configuration::WorkerConfiguration;
use crate::resources::{AllocationRequest, NumOfNodes, CPU_RESOURCE_NAME};
use crate::task::SerializedTaskContext;
use crate::{Priority, TaskId, WorkerId};
use smallvec::{smallvec, SmallVec};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceRequestEntry {
    pub resource: String,
    pub policy: AllocationRequest,
}

pub type ResourceRequestEntries = SmallVec<[ResourceRequestEntry; 3]>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceRequest {
    #[serde(default)]
    pub n_nodes: NumOfNodes,

    #[serde(default)]
    pub resources: ResourceRequestEntries,

    #[serde(default)]
    pub min_time: Duration,
}

impl Default for ResourceRequest {
    fn default() -> Self {
        ResourceRequest {
            n_nodes: 0,
            resources: smallvec![ResourceRequestEntry {
                resource: CPU_RESOURCE_NAME.to_string(),
                policy: AllocationRequest::Compact(1),
            }],
            min_time: Default::default(),
        }
    }
}

/// Task data that is often shared by multiple tasks.
/// It is send out-of-band in NewTasksMessage to save bandwidth and allocations.
#[derive(Deserialize, Serialize, Debug)]
pub struct SharedTaskConfiguration {
    #[serde(default)]
    pub resources: ResourceRequest,

    #[serde(default)]
    pub n_outputs: u32,

    #[serde(default)]
    pub time_limit: Option<Duration>,

    #[serde(default)]
    pub priority: Priority,

    #[serde(default)]
    pub keep: bool,

    #[serde(default)]
    pub observe: bool,
}

/// Task data that is unique for each task.
#[derive(Deserialize, Serialize, Debug)]
pub struct TaskConfiguration {
    pub id: TaskId,
    /// Index into NewTasksMessage::shared_data that contains the shared data for this task.
    pub shared_data_index: u32,

    pub task_deps: Vec<TaskId>,

    /// Opaque data that is passed by the gateway user to task launchers.
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct NewTasksMessage {
    pub tasks: Vec<TaskConfiguration>,
    pub shared_data: Vec<SharedTaskConfiguration>,
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
#[serde(tag = "op")]
pub enum FromGatewayMessage {
    NewTasks(NewTasksMessage),
    ObserveTasks(ObserveTasksMessage),
    CancelTasks(CancelTasks),
    GetTaskInfo(TaskInfoRequest),
    ServerInfo,
    StopWorker(StopWorkerRequest),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct MonitoringEventRequest {
    /// Get events after a particular id. All events are returned if `None`.
    pub after_id: Option<u32>,
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
    Running {
        worker_ids: SmallVec<[WorkerId; 1]>,
        context: SerializedTaskContext,
    },
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
            TaskState::Running { .. } => "Running",
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

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum LostWorkerReason {
    Stopped,
    ConnectionLost,
    HeartbeatLost,
    IdleTimeout,
    TimeLimitReached,
}

impl LostWorkerReason {
    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            LostWorkerReason::ConnectionLost | LostWorkerReason::HeartbeatLost
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LostWorkerMessage {
    pub worker_id: WorkerId,
    pub running_tasks: Vec<TaskId>,
    pub reason: LostWorkerReason,
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
    NewWorker(NewWorkerMessage),
    LostWorker(LostWorkerMessage),
    WorkerOverview(WorkerOverview),
    WorkerStopped,
}
