mod data;
mod ui;
mod ui_loop;
mod utils;

pub use ui_loop::start_ui_loop;

use std::time::Duration;

// The time range in which the live timeline is display ([now() - duration, now()])
const DEFAULT_LIVE_DURATION: Duration = Duration::from_secs(60 * 10);
