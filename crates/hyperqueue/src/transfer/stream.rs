use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use tako::{InstanceId, TaskId};

pub type ChannelId = u32;

#[derive(Serialize, Deserialize, Debug)]
pub struct StreamChunkHeader {
    #[serde(with = "ts_milliseconds")]
    pub time: DateTime<Utc>,
    pub task: TaskId,
    pub instance: InstanceId,
    pub channel: ChannelId,
    pub size: u64, // size == 0 indicates end of the stream
}
