use crate::server::job::{Job, JobId};
use crate::{TaskId, WorkerId};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use std::path::PathBuf;
use tako::messages::common::{ProgramDefinition, WorkerConfiguration};

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitMessage {
    pub name: String,
    pub cwd: PathBuf,
    pub spec: ProgramDefinition,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfoRequest {
    // If None then all jobs are turned
    pub job_ids: Option<Vec<JobId>>,
    pub include_program_def: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FromClientMessage {
    Submit(SubmitMessage),
    JobInfo(JobInfoRequest),
    WorkerList,
    Stop,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ToClientMessage {
    JobInfoResponse(JobInfoResponse),
    SubmitResponse(SubmitResponse),
    WorkerListResponse(WorkerListResponse),
    Error(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum JobStatus {
    Submitted,
    Waiting,
    Running,
    Finished,
    Failed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfo {
    pub id: TaskId,
    pub name: String,
    pub status: JobStatus,

    pub worker_id: Option<WorkerId>,
    pub error: Option<String>,

    pub spec: Option<ProgramDefinition>,
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
pub struct SubmitResponse {
    pub job: JobInfo,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerListResponse {
    pub workers: Vec<WorkerInfo>,
}
