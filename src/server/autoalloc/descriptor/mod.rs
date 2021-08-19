pub mod pbs;

use async_trait::async_trait;

use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::AutoAllocResult;

/// This trait represents a job manager queue into which new allocations can be scheduled.
///
/// TODO: try to remove async_trait and migrate to Pin<Box<dyn Future>>>
#[async_trait(?Send)]
pub trait QueueDescriptor {
    /// How many workers should be ideally active (both running and in allocation queue).
    fn target_scale(&self) -> u32;

    /// How many workers can be created in a single allocation.
    fn max_workers_per_alloc(&self) -> u32 {
        1
    }

    /// Schedule an allocation that will start the corresponding number of workers.
    /// Returns the string ID of the created allocation.
    async fn schedule_allocation(&self, worker_count: u64) -> AutoAllocResult<AllocationId>;

    /// Get status of an existing allocation
    async fn get_allocation_status(
        &self,
        allocation_id: &str,
    ) -> AutoAllocResult<Option<AllocationStatus>>;
}
