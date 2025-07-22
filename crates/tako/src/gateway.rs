use crate::internal::common::error::DsError;
use crate::internal::datasrv::dataobj::DataObjectId;
use crate::resources::{AllocationRequest, CPU_RESOURCE_NAME, NumOfNodes, ResourceAmount};
use crate::{InstanceId, Map, Priority, TaskId};
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};
use std::fmt::{Display, Formatter};
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

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone, Copy)]
pub enum CrashLimit {
    NeverRestart,
    MaxCrashes(u16),
    Unlimited,
}

impl Default for CrashLimit {
    fn default() -> Self {
        CrashLimit::MaxCrashes(5)
    }
}

impl Display for CrashLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CrashLimit::NeverRestart => f.write_str("never-restart"),
            CrashLimit::MaxCrashes(count) => write!(f, "{count}"),
            CrashLimit::Unlimited => f.write_str("unlimited"),
        }
    }
}

/// Task data that is often shared by multiple tasks.
/// It is sent out-of-band in NewTasksMessage to save bandwidth and allocations.
#[derive(Debug)]
pub struct SharedTaskConfiguration {
    pub resources: ResourceRequestVariants,

    pub time_limit: Option<Duration>,

    pub priority: Priority,

    pub crash_limit: CrashLimit,

    pub data_flags: TaskDataFlags,

    pub body: Box<[u8]>,
}

pub type EntryType = ThinVec<u8>;

/// Task data that is unique for each task.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TaskConfiguration {
    pub id: TaskId,
    /// Index into NewTasksMessage::shared_data that contains the shared data for this task.
    pub shared_data_index: u32,

    pub task_deps: ThinVec<TaskId>,

    /// If this task depends on a data object produced by a task X
    /// then X has to be also in task_deps, it is a responsibility of the caller
    /// to maintain the invariant.
    pub dataobj_deps: ThinVec<DataObjectId>,

    pub entry: Option<EntryType>,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
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
    /// Queue/query index into the query input vec
    pub worker_type: usize,
    /// Number of workers that should be spawned in each allocation
    pub worker_per_allocation: u32,
    /// Number of allocations to create
    pub max_allocations: u32,
}

#[derive(Debug)]
pub struct TaskSubmit {
    pub tasks: Vec<TaskConfiguration>,
    pub shared_data: Vec<SharedTaskConfiguration>,
    pub adjust_instance_id_and_crash_counters: Map<TaskId, (InstanceId, u32)>,
}
