use async_trait::async_trait;

use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::AutoAllocResult;

#[async_trait(?Send)]
pub trait QueueDescriptor {
    /// How many workers should be ideally active (running + in queue).
    fn target_scale(&self) -> u64;

    /// How many workers can be created in a single allocation.
    fn max_workers_per_alloc(&self) -> u64 {
        1
    }

    /// Schedule an allocation that will start the corresponding number of workers
    async fn schedule_allocation(&self, worker_count: u64) -> AutoAllocResult<AllocationId>;

    /// Get allocation status
    async fn get_allocation_status(
        &self,
        allocation_id: &str,
    ) -> AutoAllocResult<Option<AllocationStatus>>;
}
