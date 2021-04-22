use serde::Serialize;
use serde::Deserialize;
use crate::TaskId;
use std::path::PathBuf;
use tako::messages::common::ProgramDefinition;

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitMessage {
    pub name: String,
    pub cwd: PathBuf,
    pub spec: ProgramDefinition,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FromClientMessage {
    Submit(SubmitMessage),
    Stats,
    Stop
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ToClientMessage {
    StatsResponse(StatsResponse),
    SubmitResponse(SubmitResponse),
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
pub struct StatsResponse {
    pub workers: Vec<String>,
    pub jobs: Vec<JobInfo>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitResponse {
    pub job: JobInfo
}
