mod common;
pub mod pbs;
pub mod slurm;

use crate::common::manager::info::ManagerType;
use crate::server::autoalloc::state::AllocationId;
use crate::server::autoalloc::{Allocation, AutoAllocResult, QueueId};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tako::Map;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    manager: ManagerType,
    backlog: u32,
    workers_per_alloc: u32,
    timelimit: Duration,
    additional_args: Vec<String>,
    max_worker_count: Option<u32>,
    worker_args: Vec<String>,
    idle_timeout: Option<Duration>,
    worker_start_cmd: Option<String>,
    worker_stop_cmd: Option<String>,
}

impl QueueInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        manager: ManagerType,
        backlog: u32,
        workers_per_alloc: u32,
        timelimit: Duration,
        additional_args: Vec<String>,
        max_worker_count: Option<u32>,
        worker_args: Vec<String>,
        idle_timeout: Option<Duration>,
        worker_start_cmd: Option<String>,
        worker_stop_cmd: Option<String>,
    ) -> Self {
        Self {
            manager,
            backlog,
            workers_per_alloc,
            timelimit,
            additional_args,
            max_worker_count,
            worker_args,
            idle_timeout,
            worker_start_cmd,
            worker_stop_cmd,
        }
    }

    pub fn manager(&self) -> &ManagerType {
        &self.manager
    }

    pub fn backlog(&self) -> u32 {
        self.backlog
    }

    pub fn workers_per_alloc(&self) -> u32 {
        self.workers_per_alloc
    }

    pub fn timelimit(&self) -> Duration {
        self.timelimit
    }

    pub fn additional_args(&self) -> &[String] {
        &self.additional_args
    }

    pub fn max_worker_count(&self) -> Option<u32> {
        self.max_worker_count
    }

    pub fn worker_args(&self) -> &[String] {
        &self.worker_args
    }
}

#[derive(Debug)]
pub struct AllocationSubmissionResult {
    /// Directory containing stdout/stderr of the allocation (if submission was successful)
    /// and with debug information.
    ///
    /// It is returned always because we need to delete regularly to avoid too many directories and
    /// files being created.
    working_dir: PathBuf,
    /// ID of the created allocation, if it was successfully submitted.
    id: AutoAllocResult<AllocationId>,
}

impl AllocationSubmissionResult {
    pub fn new(id: AutoAllocResult<AllocationId>, working_dir: PathBuf) -> Self {
        Self { id, working_dir }
    }

    pub fn into_id(self) -> AutoAllocResult<AllocationId> {
        self.id
    }

    pub fn working_dir(&self) -> &Path {
        self.working_dir.as_path()
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
        started_at: Option<SystemTime>,
        finished_at: SystemTime,
    },
    Failed {
        started_at: Option<SystemTime>,
        finished_at: SystemTime,
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
