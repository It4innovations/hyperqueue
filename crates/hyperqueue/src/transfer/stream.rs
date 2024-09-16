use crate::{JobId, JobTaskId};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use tako::InstanceId;

pub type ChannelId = u32;

#[derive(Serialize, Deserialize, Debug)]
pub struct StreamTaskStart {
    pub job: JobId,
    pub task: JobTaskId,
    pub instance: InstanceId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StreamData {
    pub job: JobId,
    pub task: JobTaskId,
    pub instance: InstanceId,
    pub channel: ChannelId,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StreamTaskEnd {
    pub job: JobId,
    pub task: JobTaskId,
    pub instance: InstanceId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StreamChunkHeader {
    #[serde(with = "ts_milliseconds")]
    pub time: DateTime<Utc>,
    pub job: JobId,
    pub task: JobTaskId,
    pub instance: InstanceId,
    pub channel: ChannelId,
    pub size: u64, // size == 0 indicates end of the stream
}
