use serde::Serialize;
use serde::Deserialize;
use crate::{TaskId, WorkerId};
use std::path::PathBuf;
use tako::messages::common::{ProgramDefinition, WorkerConfiguration};
use chrono::{DateTime, Utc};

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitMessage {
    pub name: String,
    pub cwd: PathBuf,
    pub spec: ProgramDefinition,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FromClientMessage {
    Submit(SubmitMessage),
    JobList,
    WorkerList,
    Stop
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ToClientMessage {
    JobListResponse(JobListResponse),
    SubmitResponse(SubmitResponse),
    WorkerListResponse(WorkerListResponse),
    Error(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum JobState {
    Waiting,
    Finished,
    Failed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobInfo {
    pub id: TaskId,
    pub name: String,
    pub state: JobState,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub configuration: WorkerConfiguration,
    pub ended_at: Option<DateTime<Utc>>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobListResponse {
    pub workers: Vec<String>,
    pub jobs: Vec<JobInfo>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitResponse {
    pub job: JobInfo
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WorkerListResponse {
    pub workers: Vec<WorkerInfo>,
}
