use serde::Serialize;
use serde::Deserialize;
use crate::{TaskId, Map};
use crate::tako::common::ProgramDefinition;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug)]
pub struct SubmitMessage {
    pub name: String,
    pub cwd: PathBuf,
    pub spec: ProgramDefinition,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FromClientMessage {
    Submit(SubmitMessage),
    Stats
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