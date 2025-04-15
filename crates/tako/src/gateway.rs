use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Display;

use crate::internal::common::error::DsError;
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::WorkerOverview;
use crate::internal::worker::configuration::WorkerConfiguration;
use crate::resources::{
    AllocationRequest, CPU_RESOURCE_NAME, NumOfNodes, ResourceAmount, ResourceDescriptor,
};
use crate::task::SerializedTaskContext;
use crate::{InstanceId, Map, Priority, TaskId, WorkerId};
use smallvec::{SmallVec, smallvec};
use std::time::Duration;
use thin_vec::ThinVec;

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
                policy: AllocationRequest::Compact(ResourceAmount::new_units(1)),
            }],
            min_time: Default::default(),
        }
    }
}

impl ResourceRequest {
    pub fn validate(&self) -> crate::Result<()> {
        for (i, entry) in self.resources.iter().enumerate() {
            entry.policy.validate()?;
            for entry2 in &self.resources[i + 1..] {
                if entry.resource == entry2.resource {
                    return Err(DsError::GenericError(format!(
                        "Resource '{}' defined more than once",
                        entry.resource
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
pub struct ResourceRequestVariants {
    pub variants: SmallVec<[ResourceRequest; 1]>,
}

impl Default for ResourceRequestVariants {
    fn default() -> Self {
        ResourceRequestVariants {
            variants: smallvec![ResourceRequest::default()],
        }
    }
}

impl ResourceRequestVariants {
    pub fn new(variants: SmallVec<[ResourceRequest; 1]>) -> Self {
        ResourceRequestVariants { variants }
    }
    pub fn new_simple(rq: ResourceRequest) -> ResourceRequestVariants {
        ResourceRequestVariants::new(smallvec![rq])
    }
    pub fn min_time(&self) -> Duration {
        self.variants
            .iter()
            .map(|rq| rq.min_time)
            .min()
            .unwrap_or_default()
    }
}

/// Task data that is often shared by multiple tasks.
/// It is send out-of-band in NewTasksMessage to save bandwidth and allocations.
#[derive(Deserialize, Serialize, Debug)]
pub struct SharedTaskConfiguration {
    pub resources: ResourceRequestVariants,

    #[serde(default)]
    pub n_outputs: u32,

    #[serde(default)]
    pub time_limit: Option<Duration>,

    #[serde(default)]
    pub priority: Priority,

    pub crash_limit: u32,
}

/// Task data that is unique for each task.
#[derive(Deserialize, Serialize, Debug)]
pub struct TaskConfiguration {
    pub id: TaskId,
    /// Index into NewTasksMessage::shared_data that contains the shared data for this task.
    pub shared_data_index: u32,

    pub task_deps: ThinVec<TaskId>,

    /// Opaque data that is passed by the gateway user to task launchers.
    #[serde(with = "serde_bytes")]
    pub body: Box<[u8]>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct NewTasksMessage {
    pub tasks: Vec<TaskConfiguration>,
    pub shared_data: Vec<SharedTaskConfiguration>,
    pub adjust_instance_id: Map<TaskId, InstanceId>,
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

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerTypeQuery {
    pub descriptor: ResourceDescriptor,
    pub max_sn_workers: u32,            // For single-node tasks
    pub max_worker_per_allocation: u32, // For multi-node tasks
}

/* Ask scheduler for the information about how
  many workers of the given type is useful to spawn.

  In a situation that two worker types can be spawned to
  speed up a computation, but not both of them, then the priority
  is given by an order of by worker_queries, lesser index, higher priority

  Query:

  max_sn_workers defines how many of that worker type can outer system provides,
  if a big number is filled, it may be slow to compute the result.
  This is ment for single node tasks, i.e. they may or may not be in a same allocation.

  max_worker_per_allocation defines how many of that worker type
  we can get in one allocation at most.
  This is used for planning multi-node tasks.

*/
#[derive(Serialize, Deserialize, Debug)]
pub struct NewWorkerQuery {
    pub worker_queries: Vec<WorkerTypeQuery>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "op")]
pub enum FromGatewayMessage {
    NewTasks(NewTasksMessage),
    CancelTasks(CancelTasks),
    GetTaskInfo(TaskInfoRequest),
    ServerInfo,
    WorkerInfo(WorkerId),
    StopWorker(StopWorkerRequest),
    NewWorkerQuery(NewWorkerQuery),
    TryReleaseMemory,
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
        instance_id: InstanceId,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WorkerRuntimeInfo {
    SingleNodeTasks {
        assigned_tasks: u32,
        running_tasks: u32,
        is_reserved: bool,
    },
    MultiNodeTask {
        main_node: bool,
    },
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

impl Display for LostWorkerReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LostWorkerReason::Stopped => "stopped",
            LostWorkerReason::ConnectionLost => "connection lost",
            LostWorkerReason::HeartbeatLost => "heartbeat lost",
            LostWorkerReason::IdleTimeout => "idle timeout",
            LostWorkerReason::TimeLimitReached => "time limit reached",
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LostWorkerMessage {
    pub worker_id: WorkerId,
    pub running_tasks: Vec<TaskId>,
    pub reason: LostWorkerReason,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MultiNodeAllocationResponse {
    pub worker_type: usize,
    pub worker_per_allocation: u32,
    pub max_allocations: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NewWorkerAllocationResponse {
    pub single_node_allocations: Vec<usize>, // Corresponds to NewWorkerQuery::worker_queries
    pub multi_node_allocations: Vec<MultiNodeAllocationResponse>,
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
    WorkerInfo(Option<WorkerRuntimeInfo>),
    NewWorker(NewWorkerMessage),
    LostWorker(LostWorkerMessage),
    WorkerOverview(WorkerOverview),
    WorkerStopped,
    NewWorkerAllocationQueryResponse(NewWorkerAllocationResponse),
}
