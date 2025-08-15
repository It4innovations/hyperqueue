use crate::common::serialization::Serialized;
use crate::server::autoalloc::QueueId;
use crate::server::autoalloc::{AllocationId, QueueParameters};
use crate::transfer::messages::{JobDescription, SubmitRequest};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::rc::Rc;
use std::sync::Arc;
use tako::gateway::LostWorkerReason;
use tako::worker::{WorkerConfiguration, WorkerOverview};
use tako::{InstanceId, TaskId, static_assert_size};
use tako::{JobId, WorkerId};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EventPayload {
    /// New worker has connected to the server
    WorkerConnected(WorkerId, Box<WorkerConfiguration>),
    /// Worker has disconnected from the server
    WorkerLost(WorkerId, LostWorkerReason),
    /// Worker has proactively send its overview (task status and HW utilization report) to the server
    WorkerOverviewReceived(Box<WorkerOverview>),
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
        task_id: TaskId,
        instance_id: InstanceId,
        workers: SmallVec<[WorkerId; 1]>,
    },
    /// Task has been finished
    TaskFinished {
        task_id: TaskId,
    },
    /// Task has failed to execute
    TaskFailed {
        task_id: TaskId,
        error: String,
    },
    /// Tasks has been canceled; for performance and correctness reason, this even is batched.
    TasksCanceled {
        task_ids: Vec<TaskId>,
        message: Arc<String>,
    },
    /// New allocation queue has been created
    AllocationQueueCreated(QueueId, Box<QueueParameters>),
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
static_assert_size!(EventPayload, 40);
