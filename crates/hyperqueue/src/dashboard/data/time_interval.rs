use crate::dashboard::data::Time;
use std::ops::Sub;
use std::time::{Duration, SystemTime};

#[derive(Copy, Clone, Debug)]
pub struct TimeRange {
    pub start: Time,
    pub end: Time,
}

impl TimeRange {
    pub fn new(start: Time, end: Time) -> Self {
        assert!(start <= end);
        Self { start, end }
    }
}

impl Default for TimeRange {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            start: now - Duration::from_secs(30 * 60),
            end: now,
        }
    }
}

/// Decides how will the currently active time range be selected.
pub enum TimeMode {
    /// The latest time will always be `[now() - duration, now()]`.
    Live(Duration),
    /// The active time range is fixed.
    Fixed(TimeRange),
}

impl TimeMode {
    pub fn get_current_time(&self) -> Time {
        self.get_time_range().end
    }

    pub fn get_time_range(&self) -> TimeRange {
        match self {
            TimeMode::Live(duration) => {
                let now = SystemTime::now();
                TimeRange {
                    start: now.sub(*duration),
                    end: now,
                }
            }
            TimeMode::Fixed(range) => *range,
        }
    }
}
