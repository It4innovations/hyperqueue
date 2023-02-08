use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;

use crate::client::status::Status;
use crate::common::arraydef::IntArray;
use crate::common::manager::info::ManagerType;
use crate::server::autoalloc::{Allocation, QueueId, QueueInfo};
use crate::server::job::{JobTaskCounters, JobTaskInfo};
use crate::{JobId, JobTaskCount, JobTaskId, Map, WorkerId};
use bstr::BString;
use std::path::PathBuf;
use std::time::Duration;

use crate::server::event::MonitoringEvent;
use tako::gateway::{LostWorkerReason, MonitoringEventRequest, ResourceRequest};
use tako::program::ProgramDefinition;
use tako::worker::WorkerConfiguration;

// Messages client -> server
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug)]
pub enum FromClientMessage {
    Submit(SubmitRequest),
    Resubmit(ResubmitRequest),
    Cancel(CancelRequest),
    JobDetail(JobDetailRequest),
    JobInfo(JobInfoRequest),
    WorkerList,
    WorkerInfo(WorkerInfoRequest),
    Stats,
    StopWorker(StopWorkerMessage),
    Stop,
    AutoAlloc(AutoAllocRequest),
    WaitForJobs(WaitForJobsRequest),
    MonitoringEvents(MonitoringEventRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PinMode {
    #[serde(rename = "name")]
    None,
    #[serde(rename = "taskset")]
    TaskSet,
    #[serde(rename = "omp")]
    OpenMP,
}

impl PinMode {
    pub fn to_str(&self) -> &'static str {
        match self {
            PinMode::None => "none",
            PinMode::TaskSet => "taskset",
            PinMode::OpenMP => "openmp",
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskBody {
    pub program: ProgramDefinition,
    pub pin: PinMode,
    pub task_dir: bool,
    pub job_id: JobId,
    pub task_id: JobTaskId,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskDescription {
    pub program: ProgramDefinition,
    pub resources: ResourceRequest,
    pub pin_mode: PinMode,
    pub task_dir: bool,
    pub time_limit: Option<Duration>,
    pub priority: tako::Priority,
    pub crash_limit: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskWithDependencies {
    pub id: JobTaskId,
    pub task_desc: TaskDescription,
    pub dependencies: Vec<JobTaskId>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JobDescription {
    /// Either a single-task job or a task array usually submitted through the CLI.
    Array {
        ids: IntArray,
        entries: Option<Vec<BString>>,
        task_desc: TaskDescription,
    },
    /// Generic DAG of tasks usually submitted through the Python binding.
    Graph { tasks: Vec<TaskWithDependencies> },
}

impl JobDescription {
    pub fn task_count(&self) -> JobTaskCount {
        match self {
            JobDescription::Array { ids, .. } => ids.id_count() as JobTaskCount,
            JobDescription::Graph { tasks } => tasks.len() as JobTaskCount,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitRequest {
    pub job_desc: JobDescription,
    pub name: String,
    pub max_fails: Option<JobTaskCount>,
    pub submit_dir: PathBuf,
    pub log: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IdSelector {
    All,
    LastN(u32),
    Specific(IntArray),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SingleIdSelector {
    Specific(u32),
    Last,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TaskIdSelector {
    All,
    Specific(IntArray),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TaskStatusSelector {
    All,
    Specific(Vec<Status>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskSelector {
    pub id_selector: TaskIdSelector,
    pub status_selector: TaskStatusSelector,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResubmitRequest {
    pub job_id: JobId,
    pub filter: Vec<Status>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CancelRequest {
    pub selector: IdSelector,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfoRequest {
    pub selector: IdSelector,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobDetailRequest {
    pub job_id_selector: IdSelector,
    pub task_selector: Option<TaskSelector>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StopWorkerMessage {
    pub selector: IdSelector,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerInfoRequest {
    pub worker_id: WorkerId,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AutoAllocRequest {
    List,
    Info {
        queue_id: QueueId,
    },
    AddQueue {
        manager: ManagerType,
        parameters: AllocationQueueParams,
        dry_run: bool,
    },
    DryRun {
        manager: ManagerType,
        parameters: AllocationQueueParams,
    },
    RemoveQueue {
        queue_id: QueueId,
        force: bool,
    },
    PauseQueue {
        queue_id: QueueId,
    },
    ResumeQueue {
        queue_id: QueueId,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AllocationQueueParams {
    pub workers_per_alloc: u32,
    pub backlog: u32,
    pub timelimit: Duration,
    pub name: Option<String>,
    pub max_worker_count: Option<u32>,
    pub additional_args: Vec<String>,

    // Black-box worker args that will be passed to `worker start`
    pub worker_args: Vec<String>,
    pub idle_timeout: Option<Duration>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WaitForJobsRequest {
    pub selector: IdSelector,
}

// Messages server -> client
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug)]
pub enum ToClientMessage {
    JobInfoResponse(JobInfoResponse),
    JobDetailResponse(Vec<(JobId, Option<JobDetail>)>),
    SubmitResponse(SubmitResponse),
    WorkerListResponse(WorkerListResponse),
    WorkerInfoResponse(Option<WorkerInfo>),
    StatsResponse(StatsResponse),
    StopWorkerResponse(Vec<(WorkerId, StopWorkerResponse)>),
    CancelJobResponse(Vec<(JobId, CancelJobResponse)>),
    AutoAllocResponse(AutoAllocResponse),
    WaitForJobsResponse(WaitForJobsResponse),
    MonitoringEventsResponse(Vec<MonitoringEvent>),
    Error(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum CancelJobResponse {
    Canceled(Vec<JobTaskId>, JobTaskCount),
    InvalidJob,
    Failed(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum StopWorkerResponse {
    Stopped,
    AlreadyStopped,
    InvalidWorker,
    Failed(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StreamStats {
    pub connections: Vec<String>,
    pub registrations: Vec<(JobId, PathBuf)>,
    pub files: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatsResponse {
    pub stream_stats: StreamStats,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitResponse {
    pub job: JobDetail,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobInfo {
    pub id: JobId,
    pub name: String,

    pub n_tasks: JobTaskCount,
    pub counters: JobTaskCounters,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerExitInfo {
    pub ended_at: DateTime<Utc>,
    pub reason: LostWorkerReason,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub configuration: WorkerConfiguration,
    pub started: DateTime<Utc>,
    pub ended: Option<WorkerExitInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfoResponse {
    pub jobs: Vec<JobInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobDetail {
    pub info: JobInfo,
    pub job_desc: JobDescription,
    pub tasks: Vec<JobTaskInfo>,
    pub tasks_not_found: Vec<JobTaskId>,
    pub max_fails: Option<JobTaskCount>,

    // Date when job was submitted
    pub submission_date: DateTime<Utc>,
    pub submit_dir: PathBuf,

    // Time when job was completed or now if job is not completed
    pub completion_date_or_now: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerListResponse {
    pub workers: Vec<WorkerInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerInfoResponse {
    pub worker: WorkerInfo,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AutoAllocResponse {
    QueueCreated(QueueId),
    QueueRemoved(QueueId),
    QueuePaused(QueueId),
    QueueResumed(QueueId),
    DryRunSuccessful,
    Info(Vec<Allocation>),
    List(AutoAllocListResponse),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AutoAllocListResponse {
    pub queues: Map<QueueId, QueueData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum QueueState {
    Running,
    Paused,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueData {
    pub info: QueueInfo,
    pub name: Option<String>,
    pub manager_type: ManagerType,
    pub state: QueueState,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct WaitForJobsResponse {
    pub finished: u32,
    pub failed: u32,
    pub canceled: u32,
    pub invalid: u32,
}
