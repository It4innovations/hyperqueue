pub mod log;
pub mod payload;
pub mod streamer;

use bincode::Options;
use chrono::{DateTime, Utc};
use payload::EventPayload;
use serde::{Deserialize, Serialize};

pub type EventId = u32;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    pub time: DateTime<Utc>,
    pub payload: EventPayload,
}

#[inline]
pub(crate) fn bincode_config() -> impl Options {
    bincode::DefaultOptions::new().allow_trailing_bytes()
}
