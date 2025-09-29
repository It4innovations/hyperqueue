mod prune;
mod read;
mod stream;
mod write;

pub use read::JournalReader;
use serde::{Deserialize, Serialize};
pub use stream::{EventStreamMessage, EventStreamSender, start_event_streaming};
pub use write::JournalWriter;

const HQ_JOURNAL_HEADER: &[u8] = b"hqjl0002";

const HQ_JOURNAL_VERSION_MAJOR: u32 = 25;
const HQ_JOURNAL_VERSION_MINOR: u32 = 0;

#[derive(Serialize, Deserialize)]
struct JournalVersion {
    major: u32,
    minor: u32,
}
