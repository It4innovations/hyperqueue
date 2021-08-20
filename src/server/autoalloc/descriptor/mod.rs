pub mod pbs;

use crate::common::manager::info::ManagerType;
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::AutoAllocResult;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub struct QueueDescriptor {
    manager: ManagerType,
    info: QueueInfo,
    handler: Box<dyn QueueHandler>,
}

impl QueueDescriptor {
    pub fn new(manager: ManagerType, info: QueueInfo, handler: Box<dyn QueueHandler>) -> Self {
        Self {
            manager,
            info,
            handler,
        }
    }

    pub fn manager(&self) -> &ManagerType {
        &self.manager
    }
    pub fn info(&self) -> &QueueInfo {
        &self.info
    }
    pub fn handler(&self) -> &dyn QueueHandler {
        self.handler.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct QueueInfo {
    queue: String,
    max_workers_per_alloc: u32,
    target_worker_count: u32,
    timelimit: Option<Duration>,
}

impl QueueInfo {
    pub fn new(
        queue: String,
        max_workers_per_alloc: u32,
        target_worker_count: u32,
        timelimit: Option<Duration>,
    ) -> Self {
        QueueInfo {
            queue,
            max_workers_per_alloc,
            target_worker_count,
            timelimit,
        }
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }
    pub fn max_workers_per_alloc(&self) -> u32 {
        self.max_workers_per_alloc
    }
    pub fn target_worker_count(&self) -> u32 {
        self.target_worker_count
    }
    pub fn timelimit(&self) -> Option<Duration> {
        self.timelimit
    }
}

/// This trait represents a job manager queue into which new allocations can be scheduled.
pub trait QueueHandler {
    /// Schedule an allocation that will start the corresponding number of workers.
    /// Returns the string ID of the created allocation.
    fn schedule_allocation(
        &self,
        worker_count: u64,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationId>>>>;

    /// Get status of an existing allocation
    fn get_allocation_status(
        &self,
        allocation_id: AllocationId,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationStatus>>>>;
}
