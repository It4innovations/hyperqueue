use std::time::SystemTime;

pub use data::DashboardData;
pub use fetch::create_data_fetch_process;
pub use time_based_vec::ItemWithTime;
pub use time_interval::TimeRange;

mod data;
mod fetch;
mod time_based_vec;
mod time_interval;
pub mod timelines;

type Time = SystemTime;
