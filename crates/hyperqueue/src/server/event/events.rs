use crate::server::autoalloc::AllocationId;
use crate::server::autoalloc::QueueId;
use crate::transfer::messages::AllocationQueueParams;
use crate::transfer::messages::JobDescription;
use crate::WorkerId;
use crate::{JobId, JobTaskCount, TakoTaskId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tako::gateway::LostWorkerReason;
use tako::static_assert_size;
use tako::worker::{WorkerConfiguration, WorkerOverview};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MonitoringEventPayload {
    /// New worker has connected to the server
    WorkerConnected(WorkerId, Box<WorkerConfiguration>),
    /// Worker has disconnected from the server
    WorkerLost(WorkerId, LostWorkerReason),
    /// Worker has proactively send its overview (task status and HW utilization report) to the server
    WorkerOverviewReceived(WorkerOverview),
    /// A Job was submitted by the user.
    JobCreated(JobId, Box<JobInfo>),
    /// All tasks of the job have finished.
    JobCompleted(JobId, DateTime<Utc>),
    /// Task has started to execute on some worker
    TaskStarted {
        task_id: TakoTaskId,
        worker_id: WorkerId,
    },
    /// Task has been finished
    TaskFinished(TakoTaskId),
    // Task that failed to execute
    TaskFailed(TakoTaskId),
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobInfo {
    pub name: String,
    pub job_desc: JobDescription,

    pub base_task_id: TakoTaskId,
    pub task_ids: Vec<TakoTaskId>,
    pub max_fails: Option<JobTaskCount>,
    pub log: Option<PathBuf>,

    pub submission_date: DateTime<Utc>,
}

// Keep the size of the event structure in check
static_assert_size!(MonitoringEventPayload, 136);
