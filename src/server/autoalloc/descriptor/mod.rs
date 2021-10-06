mod common;
pub mod pbs;
pub mod slurm;

use crate::common::manager::info::ManagerType;
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::{AutoAllocResult, DescriptorId};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;

/// Describes an allocation queue. Holds both input parameters of the queue (`info`) and a handler
/// that can communicate with the queue.
pub struct QueueDescriptor {
    manager: ManagerType,
    info: QueueInfo,
    name: Option<String>,
    handler: Box<dyn QueueHandler>,
}

impl QueueDescriptor {
    pub fn new(
        manager: ManagerType,
        info: QueueInfo,
        name: Option<String>,
        handler: Box<dyn QueueHandler>,
    ) -> Self {
        Self {
            manager,
            info,
            name,
            handler,
        }
    }

    pub fn manager(&self) -> &ManagerType {
        &self.manager
    }
    pub fn info(&self) -> &QueueInfo {
        &self.info
    }
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    pub fn handler(&self) -> &dyn QueueHandler {
        self.handler.as_ref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    queue: String,
    backlog: u32,
    workers_per_alloc: u32,
    timelimit: Option<Duration>,
}

impl QueueInfo {
    pub fn new(
        queue: String,
        backlog: u32,
        workers_per_alloc: u32,
        timelimit: Option<Duration>,
    ) -> Self {
        QueueInfo {
            queue,
            backlog,
            workers_per_alloc,
            timelimit,
        }
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }
    pub fn backlog(&self) -> u32 {
        self.backlog
    }
    pub fn workers_per_alloc(&self) -> u32 {
        self.workers_per_alloc
    }
    pub fn timelimit(&self) -> Option<Duration> {
        self.timelimit
    }
}

#[derive(Debug)]
pub struct CreatedAllocation {
    id: AllocationId,
    working_dir: PathBuf,
}

impl CreatedAllocation {
    pub fn new(id: AllocationId, working_dir: PathBuf) -> Self {
        Self { id, working_dir }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn working_dir(&self) -> &Path {
        self.working_dir.as_path()
    }
}

/// Handler that can communicate with some allocation queue (e.g. PBS/Slurm queue)
pub trait QueueHandler {
    /// Schedule an allocation that will start the corresponding number of workers.
    fn schedule_allocation(
        &self,
        descriptor_id: DescriptorId,
        queue_info: &QueueInfo,
        worker_count: u64,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<CreatedAllocation>>>>;

    /// Get status of an existing allocation
    /// TODO: get status of multiple allocations to amortize qstat cost
    fn get_allocation_status(
        &self,
        allocation_id: AllocationId,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<Option<AllocationStatus>>>>>;

    /// Remove allocation, if it still exists.
    fn remove_allocation(
        &self,
        allocation_id: AllocationId,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>>;
}
