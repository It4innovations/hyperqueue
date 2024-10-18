mod data;
mod events;
mod ui;
mod ui_loop;
mod utils;

pub use ui_loop::start_ui_loop;

use std::time::Duration;

const DEFAULT_LIVE_DURATION: Duration = Duration::from_secs(60 * 10);
