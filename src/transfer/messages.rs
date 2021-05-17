use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use tako::messages::common::{ProgramDefinition, WorkerConfiguration};

use crate::common::arraydef::ArrayDef;
use crate::server::job::{JobTaskCounters, JobTaskInfo};
use crate::{JobId, JobTaskCount, JobTaskId, WorkerId};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JobType {
    Simple,
    Array(ArrayDef),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitRequest {
    pub job_type: JobType,
    pub name: String,
    pub submit_cwd: PathBuf,
    pub spec: ProgramDefinition,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CancelRequest {
    pub job_id: JobId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfoRequest {
    // If None then all jobs are turned
    pub job_ids: Option<Vec<JobId>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobDetailRequest {
    pub job_id: JobId,
    pub include_tasks: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StopWorkerMessage {
    pub(crate) worker_id: WorkerId,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FromClientMessage {
    Submit(SubmitRequest),
    Cancel(CancelRequest),
    JobDetail(JobDetailRequest),
    JobInfo(JobInfoRequest),
    WorkerList,
    StopWorker(StopWorkerMessage),
    Stop,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum CancelJobResponse {
    Canceled(Vec<JobTaskId>, JobTaskCount),
    InvalidJob,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ToClientMessage {
    JobInfoResponse(JobInfoResponse),
    JobDetailResponse(Option<JobDetail>),
    SubmitResponse(SubmitResponse),
    WorkerListResponse(WorkerListResponse),
    StopWorkerResponse,
    CancelJobResponse(CancelJobResponse),
    Error(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TaskStatus {
    Waiting,
    Running,
    Finished,
    Failed(String),
    Canceled,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfo {
    pub id: JobId,
    pub name: String,

    pub n_tasks: JobTaskCount,
    pub counters: JobTaskCounters,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub configuration: WorkerConfiguration,
    pub ended_at: Option<DateTime<Utc>>,
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
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitResponse {
    pub job: JobDetail,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerListResponse {
    pub workers: Vec<WorkerInfo>,
}
