use crate::dashboard::data::Time;
use chrono::{TimeZone, Utc};
use std::fmt::{Display, Formatter};
use std::ops::{Add, Sub};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Copy, Clone, Debug)]
pub struct TimeRange {
    start: Time,
    end: Time,
}

impl TimeRange {
    pub fn new(start: Time, end: Time) -> Self {
        assert!(start <= end);
        Self { start, end }
    }

    pub fn start(&self) -> Time {
        self.start
    }

    pub fn end(&self) -> Time {
        self.end
    }

    pub fn sooner(&self, duration: Duration) -> Self {
        TimeRange::new(self.start.sub(duration), self.end.sub(duration))
    }

    pub fn later(&self, duration: Duration) -> Self {
        TimeRange::new(self.start.add(duration), self.end.add(duration))
    }

    pub fn duration(&self) -> Duration {
        self.end.duration_since(self.start).unwrap()
    }

    pub fn generate_steps(&self, count: u64) -> impl Iterator<Item = Time> {
        assert!(count > 0);

        let mut time = self.start;
        let duration = self.duration() / count as u32;
        std::iter::from_fn(move || {
            if time > self.end {
                None
            } else {
                let next = time;
                time += duration;
                Some(next)
            }
        })
    }
}

impl Display for TimeRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{:?}, {:?}]",
            Utc.timestamp_opt(
                self.start.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                0
            )
            .unwrap(),
            Utc.timestamp_opt(
                self.end.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                0
            )
            .unwrap()
        )
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
