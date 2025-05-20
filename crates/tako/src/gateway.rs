use serde::{Deserialize, Serialize};
use std::fmt::Display;

use crate::internal::common::error::DsError;
use crate::internal::datasrv::dataobj::DataObjectId;
use crate::resources::{
    AllocationRequest, CPU_RESOURCE_NAME, NumOfNodes, ResourceAmount,
};
use crate::{InstanceId, Map, Priority, TaskId};
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

bitflags::bitflags! {
    #[derive(Debug, Copy, Clone, Serialize, Deserialize)]
    #[cfg_attr(test, derive(Eq, PartialEq))]
    #[serde(transparent)]
    pub struct TaskDataFlags: u32 {
        const ENABLE_DATA_LAYER = 0b00000001;
    }
}

/// Task data that is often shared by multiple tasks.
/// It is sent out-of-band in NewTasksMessage to save bandwidth and allocations.
#[derive(Deserialize, Serialize, Debug)]
pub struct SharedTaskConfiguration {
    pub resources: ResourceRequestVariants,

    pub time_limit: Option<Duration>,

    pub priority: Priority,

    pub crash_limit: u32,

    pub data_flags: TaskDataFlags,
}

/// Task data that is unique for each task.
#[derive(Deserialize, Serialize, Debug)]
pub struct TaskConfiguration {
    pub id: TaskId,
    /// Index into NewTasksMessage::shared_data that contains the shared data for this task.
    pub shared_data_index: u32,

    pub task_deps: ThinVec<TaskId>,

    /// If this task depends on a data object produced by a task X
    /// then X has to be also in task_deps, it is a responsibility of the caller
    /// to maintain the invariant.
    pub dataobj_deps: ThinVec<DataObjectId>,

    /// Opaque data that is passed by the gateway user to task launchers.
    #[serde(with = "serde_bytes")]
    pub body: Box<[u8]>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug)]
pub struct MultiNodeAllocationResponse {
    pub worker_type: usize,
    pub worker_per_allocation: u32,
    pub max_allocations: u32,
}

#[derive(Debug)]
pub struct TaskSubmit {
    pub tasks: Vec<TaskConfiguration>,
    pub shared_data: Vec<SharedTaskConfiguration>,
    pub adjust_instance_id_and_crash_counters: Map<TaskId, (InstanceId, u32)>,
}

//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct TaskInfoRequest {
//     pub tasks: Vec<TaskId>, // If empty, then all tasks are assumed
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct CancelTasks {
//     pub tasks: Vec<TaskId>, // If empty, then all tasks are assumed
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct StopWorkerRequest {
//     pub worker_id: WorkerId,
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct WorkerTypeQuery {
//     pub descriptor: ResourceDescriptor,
//     pub max_sn_workers: u32,            // For single-node tasks
//     pub max_worker_per_allocation: u32, // For multi-node tasks
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct NewWorkerQuery {
//     pub worker_queries: Vec<WorkerTypeQuery>,
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub enum WorkerOverviewListenerOp {
//     Add,
//     Remove,
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// #[serde(tag = "op")]
// pub enum FromGatewayMessage {
//     NewTasks(NewTasksMessage),
//     CancelTasks(CancelTasks),
//     GetTaskInfo(TaskInfoRequest),
//     ServerInfo,
//     WorkerInfo(WorkerId),
//     StopWorker(StopWorkerRequest),
//     NewWorkerQuery(NewWorkerQuery),
//     TryReleaseMemory,
//     ModifyWorkerOverviewListeners(WorkerOverviewListenerOp),
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct NewTasksResponse {
//     pub n_waiting_for_workers: u64,
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct ErrorResponse {
//     pub message: String,
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct TaskUpdate {
//     pub id: TaskId,
//     pub state: TaskState,
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct TaskFailedMessage {
//     pub id: TaskId,
//     pub cancelled_tasks: Vec<TaskId>,
//     pub info: TaskFailInfo,
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct ServerInfo {
//     pub worker_listen_port: u16,
// }
//
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct TaskInfo {
//     pub id: TaskId,
//     pub state: TaskState,
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct TasksInfoResponse {
//     pub tasks: Vec<TaskInfo>,
// }
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct NewWorkerMessage {
//     pub worker_id: WorkerId,
//     pub configuration: WorkerConfiguration,
// }
//
//
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct LostWorkerMessage {
//     pub worker_id: WorkerId,
//     pub running_tasks: Vec<TaskId>,
//     pub reason: LostWorkerReason,
// }
//
//
// #[derive(Serialize, Deserialize, Debug)]
// #[allow(clippy::large_enum_variant)] // This Enum will be removed soon
// pub enum ToGatewayMessage {
//     NewTasksResponse(NewTasksResponse),
//     CancelTasksResponse(CancelTasksResponse),
//     TaskUpdate(TaskUpdate),
//     TaskFailed(TaskFailedMessage),
//     TaskInfo(TasksInfoResponse),
//     Error(ErrorResponse),
//     ServerInfo(ServerInfo),
//     WorkerInfo(Option<WorkerRuntimeInfo>),
//     NewWorker(NewWorkerMessage),
//     LostWorker(LostWorkerMessage),
//     WorkerOverview(Box<WorkerOverview>),
//     WorkerStopped,
//     NewWorkerAllocationQueryResponse(NewWorkerAllocationResponse),
// }
