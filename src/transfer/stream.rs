use crate::{JobId, JobTaskId};
use serde::Deserialize;
use serde::Serialize;
use tako::InstanceId;

pub type ChannelId = u32;

#[derive(Serialize, Deserialize, Debug)]
pub struct StreamRegistration {
    pub job: JobId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartTaskStreamMsg {
    pub task: JobTaskId,
    pub instance: InstanceId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataMsg {
    pub task: JobTaskId,
    pub instance: InstanceId,
    pub channel: ChannelId,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EndTaskStreamMsg {
    pub task: JobTaskId,
    pub instance: InstanceId,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FromStreamerMessage {
    Start(StartTaskStreamMsg),
    Data(DataMsg),
    End(EndTaskStreamMsg),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EndTaskStreamResponseMsg {
    pub task: JobTaskId,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ToStreamerMessage {
    Error(String),
    EndResponse(EndTaskStreamResponseMsg),
}
