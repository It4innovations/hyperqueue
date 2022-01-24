use crate::server::autoalloc::AllocationId;
use crate::server::autoalloc::DescriptorId;
use crate::transfer::messages::AllocationQueueParams;
use crate::WorkerId;
use serde::{Deserialize, Serialize};
use tako::messages::common::WorkerConfiguration;
use tako::messages::gateway::LostWorkerReason;
use tako::messages::worker::WorkerOverview;
use tako::{static_assert_size, TaskId};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MonitoringEventPayload {
    // Workers
    WorkerConnected(WorkerId, Box<WorkerConfiguration>),
    WorkerLost(WorkerId, LostWorkerReason),
    WorkerOverviewReceived(WorkerOverview),
    // Tasks
    TaskStarted {
        task_id: TaskId,
        worker_id: WorkerId,
    },
    TaskFinished(TaskId),
    // Allocations
    AllocationQueueCreated(DescriptorId, Box<AllocationQueueParams>),
    AllocationQueueRemoved(DescriptorId),
    AllocationQueued {
        allocation_id: AllocationId,
        worker_count: u64,
    },
    AllocationStarted(AllocationId),
    AllocationFinished(AllocationId),
}

// Keep the size of the event structure in check
static_assert_size!(MonitoringEventPayload, 136);
