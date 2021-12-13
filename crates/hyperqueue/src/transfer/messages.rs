use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use tako::messages::common::{ProgramDefinition, WorkerConfiguration};

use crate::client::status::Status;
use crate::common::arraydef::IntArray;
use crate::common::manager::info::ManagerType;
use crate::server::autoalloc::{Allocation, AllocationEventHolder, DescriptorId, QueueInfo};
use crate::server::job::{JobTaskCounters, JobTaskInfo};
use crate::{JobId, JobTaskCount, JobTaskId, Map, WorkerId};
use bstr::BString;
use std::path::PathBuf;
use std::time::Duration;
use tako::messages::gateway::{CollectedOverview, OverviewRequest, ResourceRequest};

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
    Overview(OverviewRequest),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskBody {
    pub program: ProgramDefinition,
    pub pin: bool,
    pub job_id: JobId,
    pub task_id: JobTaskId,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JobType {
    Simple,
    Array(IntArray),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitRequest {
    pub job_type: JobType,
    pub name: String,
    pub max_fails: Option<JobTaskCount>,
    pub spec: ProgramDefinition,
    pub resources: ResourceRequest,
    pub pin: bool,
    pub entries: Option<Vec<BString>>,
    pub submit_dir: PathBuf,
    pub priority: tako::Priority,
    pub time_limit: Option<Duration>,
    pub log: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Selector {
    All,
    LastN(u32),
    Specific(IntArray),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResubmitRequest {
    pub job_id: JobId,
    pub status: Option<Vec<Status>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CancelRequest {
    pub selector: Selector,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfoRequest {
    pub selector: Selector,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobDetailRequest {
    pub selector: Selector,
    pub include_tasks: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StopWorkerMessage {
    pub selector: Selector,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerInfoRequest {
    pub worker_id: WorkerId,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AutoAllocRequest {
    List,
    Events {
        descriptor: DescriptorId,
    },
    Info {
        descriptor: DescriptorId,
    },
    AddQueue {
        manager: ManagerType,
        parameters: AllocationQueueParams,
    },
    RemoveQueue {
        descriptor: DescriptorId,
        force: bool,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AllocationQueueParams {
    pub workers_per_alloc: u32,
    pub backlog: u32,
    pub timelimit: Option<Duration>,
    pub name: Option<String>,
    pub additional_args: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WaitForJobsRequest {
    pub selector: Selector,
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
    OverviewResponse(CollectedOverview),
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

#[derive(Serialize, Deserialize, Debug)]
pub enum JobStatus {
    Submitted,
    Waiting,
    Running,
    Finished,
    Failed(String),
    Canceled,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobInfo {
    pub id: JobId,
    pub name: String,

    pub n_tasks: JobTaskCount,
    pub counters: JobTaskCounters,
    pub resources: ResourceRequest,
}

// We need to duplicate LostWorkerReason because of serialization problems (msgpack vs. binpack)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LostWorkerReasonInfo {
    Stopped,
    ConnectionLost,
    HeartbeatLost,
    IdleTimeout,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerExitInfo {
    pub ended_at: DateTime<Utc>,
    pub reason: LostWorkerReasonInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub configuration: WorkerConfiguration,
    pub ended: Option<WorkerExitInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfoResponse {
    pub jobs: Vec<JobInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobDetail {
    pub info: JobInfo,
    pub job_type: JobType,
    pub program_def: ProgramDefinition,
    pub tasks: Vec<JobTaskInfo>,
    pub resources: ResourceRequest,
    pub pin: bool,
    pub max_fails: Option<JobTaskCount>,
    pub priority: tako::Priority,
    pub time_limit: Option<Duration>,

    // Date when job was submitted
    pub submission_date: DateTime<Utc>,

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
    QueueCreated(DescriptorId),
    QueueRemoved(DescriptorId),
    Events(Vec<AllocationEventHolder>),
    Info(Vec<Allocation>),
    List(AutoAllocListResponse),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AutoAllocListResponse {
    pub descriptors: Map<DescriptorId, QueueDescriptorData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueDescriptorData {
    pub info: QueueInfo,
    pub name: Option<String>,
    pub manager_type: ManagerType,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct WaitForJobsResponse {
    pub finished: u32,
    pub failed: u32,
    pub canceled: u32,
    pub invalid: u32,
}
