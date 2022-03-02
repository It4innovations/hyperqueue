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
    /// New worker has connected to the server
    WorkerConnected(WorkerId, Box<WorkerConfiguration>),
    /// Worker has disconnected from the server
    WorkerLost(WorkerId, LostWorkerReason),
    /// Worker has proactively send its overview (task status and HW utilization report) to the server
    WorkerOverviewReceived(WorkerOverview),
    /// Task has started to execute on some worker
    TaskStarted {
        task_id: TaskId,
        worker_id: WorkerId,
    },
    /// Task has been finished
    TaskFinished(TaskId),
    // Task that failed to execute
    TaskFailed(TaskId),
    /// New allocation queue has been created
    AllocationQueueCreated(DescriptorId, Box<AllocationQueueParams>),
    /// Allocation queue has been removed
    AllocationQueueRemoved(DescriptorId),
    /// Allocation was submitted into PBS/Slurm
    AllocationQueued {
        descriptor_id: DescriptorId,
        allocation_id: AllocationId,
        worker_count: u64,
    },
    /// PBS/Slurm allocation started executing
    AllocationStarted(DescriptorId, AllocationId),
    /// PBS/Slurm allocation has finished executing
    AllocationFinished(DescriptorId, AllocationId),
}

// Keep the size of the event structure in check
static_assert_size!(MonitoringEventPayload, 136);
