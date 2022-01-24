mod read;
mod stream;
mod write;

pub use read::EventLogReader;
pub use stream::{start_event_streaming, EventStreamSender};
pub use write::EventLogWriter;

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
