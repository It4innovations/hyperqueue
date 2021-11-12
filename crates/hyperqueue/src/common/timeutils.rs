use std::time::SystemTime;

use chrono::TimeZone;
use std::time::Duration;

crate::arg_wrapper!(ArgDuration, Duration, humantime::parse_duration);

pub fn local_to_system_time(datetime: chrono::NaiveDateTime) -> SystemTime {
    chrono::Local.from_local_datetime(&datetime).unwrap().into()
}
