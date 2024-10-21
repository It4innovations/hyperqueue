pub mod log;
pub mod payload;
pub mod streamer;

use crate::stream::StreamSerializationConfig;
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use payload::EventPayload;
use serde::{Deserialize, Serialize};

pub type EventId = u32;

type EventSerializationConfig = StreamSerializationConfig;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    #[serde(with = "ts_milliseconds")]
    pub time: DateTime<Utc>,
    pub payload: EventPayload,
}
