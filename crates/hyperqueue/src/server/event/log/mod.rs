mod read;
mod stream;
mod write;

pub use read::EventLogReader;
pub use stream::{start_event_streaming, EventStreamSender};
pub use write::EventLogWriter;

use crate::server::event::MonitoringEventId;
use bstr::BString;
use serde::{Deserialize, Serialize};

const HQ_LOG_HEADER: &[u8] = b"hq-event-log";
const HQ_LOG_VERSION: u32 = 0;

fn canonical_header() -> LogFileHeader {
    LogFileHeader {
        header: HQ_LOG_HEADER.into(),
        version: HQ_LOG_VERSION,
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct LogFileHeader {
    header: BString,
    version: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EventChunkHeader {
    /// The first event id in the compressed event chunk.
    pub first_event_id: MonitoringEventId,
    /// The compressed size in bytes of the event chunk.
    pub chunk_length: usize,
}
