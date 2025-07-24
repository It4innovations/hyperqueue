mod common;
pub mod pbs;
pub mod slurm;

use crate::common::manager::info::ManagerType;
use crate::common::utils::time::AbsoluteTime;
use crate::server::autoalloc::state::{AllocationId, AllocationWorkdir};
use crate::server::autoalloc::{Allocation, AutoAllocResult, QueueId};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tako::Map;
use tako::resources::ResourceDescriptor;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueueParameters {
    pub manager: ManagerType,
    pub max_workers_per_alloc: u32,
    pub backlog: u32,
    pub timelimit: Duration,
    pub name: Option<String>,
    pub max_worker_count: Option<u32>,
    pub min_utilization: Option<f32>,
    pub additional_args: Vec<String>,

    pub worker_start_cmd: Option<String>,
    pub worker_stop_cmd: Option<String>,

    /// Resources descriptor constructed from worker CLI options
    pub cli_resource_descriptor: Option<ResourceDescriptor>,

    // Black-box worker args that will be passed to `worker start`
    pub worker_args: Vec<String>,
    pub idle_timeout: Option<Duration>,
}

// Wrapper that exposes some methods and doesn't allow pub access to the queue parameter
// fields.
#[derive(Debug, Clone)]
pub struct QueueInfo(QueueParameters);

impl QueueInfo {
    pub fn new(params: QueueParameters) -> Self {
        Self(params)
    }

    pub fn params(&self) -> &QueueParameters {
        &self.0
    }

    pub fn manager(&self) -> &ManagerType {
        &self.0.manager
    }

    pub fn backlog(&self) -> u32 {
        self.0.backlog
    }

    pub fn max_workers_per_alloc(&self) -> u32 {
        self.0.max_workers_per_alloc
    }

    pub fn timelimit(&self) -> Duration {
        self.0.timelimit
    }

    pub fn additional_args(&self) -> &[String] {
        &self.0.additional_args
    }

    pub fn max_worker_count(&self) -> Option<u32> {
        self.0.max_worker_count
    }

    pub fn worker_args(&self) -> &[String] {
        &self.0.worker_args
    }

    pub fn min_utilization(&self) -> Option<f32> {
        self.0.min_utilization
    }

    pub fn cli_resource_descriptor(&self) -> Option<&ResourceDescriptor> {
        self.0.cli_resource_descriptor.as_ref()
    }
}

#[derive(Debug)]
pub struct AllocationSubmissionResult {
    /// Directory containing stdout/stderr of the allocation (if submission was successful)
    /// and with debug information.
    ///
    /// It is returned always because we need to delete regularly to avoid too many directories and
    /// files being created.
    working_dir: AllocationWorkdir,
    /// ID of the created allocation, if it was successfully submitted.
    id: AutoAllocResult<AllocationId>,
}

impl AllocationSubmissionResult {
    pub fn new(id: AutoAllocResult<AllocationId>, working_dir: AllocationWorkdir) -> Self {
        Self { id, working_dir }
    }

    pub fn into_id(self) -> AutoAllocResult<AllocationId> {
        self.id
    }

    pub fn working_dir(&self) -> &AllocationWorkdir {
        &self.working_dir
    }
}

pub enum SubmitMode {
    /// Submit an allocation in a normal way.
    Submit,
    /// Submit an allocation only to test queue parameters.
    DryRun,
}

#[derive(Clone, Debug)]
pub enum AllocationExternalStatus {
    Queued,
    Running,
    Finished {
        started_at: Option<AbsoluteTime>,
        finished_at: AbsoluteTime,
    },
    Failed {
        started_at: Option<AbsoluteTime>,
        finished_at: AbsoluteTime,
    },
}

pub type AllocationStatusMap = Map<AllocationId, AutoAllocResult<AllocationExternalStatus>>;

/// Handler that can communicate with some allocation queue (e.g. PBS/Slurm queue)
pub trait QueueHandler {
    /// Submit an allocation that will start the corresponding number of workers.
    ///
    /// If the method returns an error, no directory was created on disk.
    /// If it returns Ok, the directory was created and submission result can be read out of the
    /// `id` field of `AllocationSubmissionResult`.
    fn submit_allocation(
        &mut self,
        queue_id: QueueId,
        queue_info: &QueueInfo,
        worker_count: u64,
        mode: SubmitMode,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationSubmissionResult>>>>;

    /// Get statuses of a set of existing allocations.
    /// This function takes multiple allocations at once to amortize the query cost.
    fn get_status_of_allocations(
        &self,
        allocations: &[&Allocation],
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationStatusMap>>>>;

    /// Remove allocation, if it still exists.
    fn remove_allocation(
        &self,
        allocation: &Allocation,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>>;
}
