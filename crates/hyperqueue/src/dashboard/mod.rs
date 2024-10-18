mod data;
mod events;
mod ui;
mod ui_loop;
mod utils;

pub use ui_loop::start_ui_loop;

use std::time::Duration;

// The time range in which the live timeline is display ([now() - duration, now()])
const DEFAULT_LIVE_DURATION: Duration = Duration::from_secs(60 * 10);

// The amount of time that is subtracted/added when moving the timeline
const TIMELINE_MOVE_OFFSET: Duration = Duration::from_secs(60 * 5);
