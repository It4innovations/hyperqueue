use crate::dashboard::data::time_interval::TimeRange;
use crate::dashboard::data::Time;

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ItemWithTime<T> {
    pub time: Time,
    pub item: T,
}

/// Vec that has its elements sorted by time.
pub struct TimeBasedVec<T> {
    // Invariant: the items are sorted by time
    items: Vec<ItemWithTime<T>>,
}

impl<T> TimeBasedVec<T> {
    pub fn push(&mut self, time: Time, item: T) {
        // Fast path: we're appending an event that should be at the end of the vec
        if self
            .items
            .last()
            .map(|item| item.time < time)
            .unwrap_or(true)
        {
            self.items.push(ItemWithTime { time, item });
        } else {
            let index = self.items.partition_point(|item| item.time < time);
            self.items.insert(index, ItemWithTime { time, item });
        }
    }

    /// Returns the most recent item that has happened before or at `time`.
    pub fn get_most_recent_at(&self, time: Time) -> Option<&ItemWithTime<T>> {
        let index = self.items.partition_point(|item| item.time <= time);
        if index == 0 {
            None
        } else {
            self.items.get(index - 1)
        }
    }

    /// Returns the items that have happened between `start` and `end` (both inclusive).
    pub fn get_time_range(&self, range: TimeRange) -> &[ItemWithTime<T>] {
        let start_index = self.items.partition_point(|item| item.time < range.start());
        let end_index = self.items.partition_point(|item| item.time <= range.end());

        &self.items[start_index..end_index]
    }

    #[cfg(test)]
    fn into_vec(self) -> Vec<T> {
        self.into()
    }
}

#[cfg(test)]
impl<T> From<TimeBasedVec<T>> for Vec<T> {
    fn from(value: TimeBasedVec<T>) -> Self {
        value.items.into_iter().map(|item| item.item).collect()
    }
}

impl<T> Default for TimeBasedVec<T> {
    fn default() -> Self {
        Self {
            items: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dashboard::data::time_based_vec::{ItemWithTime, Time, TimeBasedVec};
    use crate::dashboard::data::time_interval::TimeRange;
    use std::ops::{Add, Sub};
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_insert_into_empty() {
        let mut vec = TimeBasedVec::default();
        vec.push(now(), 1);

        assert_eq!(vec.into_vec(), vec![1]);
    }

    #[test]
    fn test_insert_at_end() {
        let mut vec = TimeBasedVec::default();
        vec.push(now(), 1);
        vec.push(hours(1), 2);

        assert_eq!(vec.into_vec(), vec![1, 2]);
    }

    #[test]
    fn test_insert_at_beginning() {
        let mut vec = TimeBasedVec::default();
        vec.push(hours(1), 2);
        vec.push(now(), 1);

        assert_eq!(vec.into_vec(), vec![1, 2]);
    }

    #[test]
    fn test_insert_at_middle() {
        let mut vec = TimeBasedVec::default();
        vec.push(now(), 1);
        vec.push(hours(1), 2);
        vec.push(hours(3), 4);
        vec.push(hours(2), 3);

        assert_eq!(vec.into_vec(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_get_most_recent_exact_beginning() {
        let time = now();

        let mut vec = TimeBasedVec::default();
        vec.push(time, 1);
        vec.push(hours(1), 1);

        assert_eq!(
            vec.get_most_recent_at(time),
            Some(&ItemWithTime { time, item: 1 })
        );
    }

    #[test]
    fn test_get_most_recent_exact_middle() {
        let time = now();

        let mut vec = TimeBasedVec::default();
        vec.push(hours_past(1), 2);
        vec.push(time, 1);
        vec.push(hours(1), 3);

        assert_eq!(
            vec.get_most_recent_at(time),
            Some(&ItemWithTime { time, item: 1 })
        );
    }

    #[test]
    fn test_get_most_recent_exact_end() {
        let time = now();

        let mut vec = TimeBasedVec::default();
        vec.push(hours_past(1), 1);
        vec.push(time, 1);

        assert_eq!(
            vec.get_most_recent_at(time),
            Some(&ItemWithTime { time, item: 1 })
        );
    }

    #[test]
    fn test_get_most_recent_too_early() {
        let mut vec = TimeBasedVec::default();
        vec.push(now(), 1);
        vec.push(hours(1), 2);

        assert_eq!(vec.get_most_recent_at(hours_past(100)), None);
    }

    #[test]
    fn test_get_most_recent_beginning() {
        let time = now();

        let mut vec = TimeBasedVec::default();
        vec.push(time, 1);
        vec.push(hours(10), 2);

        assert_eq!(
            vec.get_most_recent_at(hours(1)),
            Some(&ItemWithTime { time, item: 1 })
        );
    }

    #[test]
    fn test_get_most_recent_middle() {
        let time = hours(2);

        let mut vec = TimeBasedVec::default();
        vec.push(now(), 1);
        vec.push(time, 2);
        vec.push(hours(10), 3);

        assert_eq!(
            vec.get_most_recent_at(hours(3)),
            Some(&ItemWithTime { time, item: 2 })
        );
    }

    #[test]
    fn test_get_most_recent_end() {
        let time = hours(1);

        let mut vec = TimeBasedVec::default();
        vec.push(now(), 1);
        vec.push(time, 2);

        assert_eq!(
            vec.get_most_recent_at(hours(2)),
            Some(&ItemWithTime { time, item: 2 })
        );
    }

    #[test]
    fn test_get_range_too_early() {
        check_range(&[], hours_past(2), hours_past(1), &[]);
        check_range(&[(now(), 1)], hours_past(2), hours_past(1), &[]);
    }

    #[test]
    fn test_get_range_too_late() {
        check_range(&[], hours(1), hours(2), &[]);
        check_range(&[(now(), 1)], hours(1), hours(2), &[]);
    }

    #[test]
    fn test_get_range_gap() {
        check_range(&[(now(), 1), (hours(3), 2)], hours(1), hours(2), &[]);
    }

    #[test]
    fn test_get_range_start_inclusive() {
        let t1 = now();
        check_range(&[(t1, 1), (hours(3), 2)], t1, hours(2), &[1]);
    }

    #[test]
    fn test_get_range_end_inclusive() {
        let t1 = hours(2);
        check_range(&[(hours_past(1), 1), (t1, 2)], hours(1), t1, &[2]);
    }

    #[test]
    fn test_get_range_start_early() {
        check_range(
            &[(now(), 1), (hours(1), 2), (hours(3), 3)],
            hours_past(1),
            hours(2),
            &[1, 2],
        );
    }

    #[test]
    fn test_get_range_end_late() {
        check_range(
            &[(now(), 1), (hours(1), 2), (hours(3), 3)],
            hours(2),
            hours(5),
            &[3],
        );
    }

    #[test]
    fn test_get_range_start_early_end_late() {
        check_range(
            &[(now(), 1), (hours(1), 2), (hours(3), 3)],
            hours_past(1),
            hours(5),
            &[1, 2, 3],
        );
    }

    fn check_range(items: &[(Time, u32)], start: Time, end: Time, result: &[u32]) {
        let mut vec = TimeBasedVec::default();
        for item in items {
            vec.push(item.0, item.1);
        }
        let range: Vec<_> = vec
            .get_time_range(TimeRange::new(start, end))
            .iter()
            .map(|i| i.item)
            .collect();
        assert_eq!(range, result.to_vec());
    }

    fn hours(hours: u64) -> Time {
        now().add(Duration::from_secs(3600 * hours))
    }

    fn hours_past(hours: u64) -> Time {
        now().sub(Duration::from_secs(3600 * hours))
    }

    fn now() -> Time {
        SystemTime::now()
    }
}
