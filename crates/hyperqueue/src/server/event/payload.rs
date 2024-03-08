use crate::server::autoalloc::AllocationId;
use crate::server::autoalloc::QueueId;
use crate::transfer::messages::{AllocationQueueParams, JobDescription};
use crate::JobId;
use crate::{JobTaskId, WorkerId};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::rc::Rc;
use tako::gateway::LostWorkerReason;
use tako::worker::{WorkerConfiguration, WorkerOverview};
use tako::{static_assert_size, InstanceId};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EventPayload {
    /// New worker has connected to the server
    WorkerConnected(WorkerId, Box<WorkerConfiguration>),
    /// Worker has disconnected from the server
    WorkerLost(WorkerId, LostWorkerReason),
    /// Worker has proactively send its overview (task status and HW utilization report) to the server
    WorkerOverviewReceived(WorkerOverview),
    /// A Job was submitted by the user -- full information to reconstruct the job;
    ///  it will be only stored into file, not held in memory
    ///  Vec<u8> is serialized JobDescription; the main reason is avoid duplication of JobDescription
    ///  (we serialize it before it is stripped down)
    ///  and a nice side effect is that Events can be deserialized without deserializing a potentially large submit data
    JobCreatedFull(JobId, Vec<u8>),
    /// All tasks of the job have finished.
    JobCompleted(JobId),
    /// Task has started to execute on some worker
    TaskStarted {
        job_id: JobId,
        task_id: JobTaskId,
        instance_id: InstanceId,
        workers: SmallVec<[WorkerId; 1]>,
    },
    /// Task has been finished
    TaskFinished { job_id: JobId, task_id: JobTaskId },
    // Task that failed to execute
    TaskFailed {
        job_id: JobId,
        task_id: JobTaskId,
        error: String,
    },
    /// New allocation queue has been created
    AllocationQueueCreated(QueueId, Box<AllocationQueueParams>),
    /// Allocation queue has been removed
    AllocationQueueRemoved(QueueId),
    /// Allocation was submitted into PBS/Slurm
    AllocationQueued {
        queue_id: QueueId,
        allocation_id: AllocationId,
        worker_count: u64,
    },
    /// PBS/Slurm allocation started executing
    AllocationStarted(QueueId, AllocationId),
    /// PBS/Slurm allocation has finished executing
    AllocationFinished(QueueId, AllocationId),
}

// Keep the size of the event structure in check
static_assert_size!(EventPayload, 136);
