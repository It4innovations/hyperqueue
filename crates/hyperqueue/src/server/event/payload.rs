use crate::common::serialization::Serialized;
use crate::server::autoalloc::AllocationId;
use crate::server::autoalloc::QueueId;
use crate::transfer::messages::{AllocationQueueParams, JobDescription, SubmitRequest};
use crate::JobId;
use crate::{JobTaskId, WorkerId};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
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
    ///  Vec<u8> is serialized SubmitRequest; the main reason is avoiding duplication of SubmitRequest
    ///  (we serialize it before it is stripped down)
    ///  and a nice side effect is that Events can be deserialized without deserializing a potentially large submit data
    Submit {
        job_id: JobId,
        closed_job: bool,
        serialized_desc: Serialized<SubmitRequest>,
    },
    /// All tasks of the job have finished.
    JobCompleted(JobId),
    JobOpen(JobId, JobDescription),
    JobClose(JobId),
    /// Task has started to execute on some worker
    TaskStarted {
        job_id: JobId,
        task_id: JobTaskId,
        instance_id: InstanceId,
        workers: SmallVec<[WorkerId; 1]>,
    },
    /// Task has been finished
    TaskFinished {
        job_id: JobId,
        task_id: JobTaskId,
    },
    /// Task has failed to execute
    TaskFailed {
        job_id: JobId,
        task_id: JobTaskId,
        error: String,
    },
    /// Task has been canceled
    TaskCanceled {
        job_id: JobId,
        task_id: JobTaskId,
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
    ServerStart {
        server_uid: String,
    },
    ServerStop,
}

// Keep the size of the event structure in check
static_assert_size!(EventPayload, 176);
