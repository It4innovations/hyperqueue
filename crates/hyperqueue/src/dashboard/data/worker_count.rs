use std::time::SystemTime;
use tako::server::monitoring::{MonitoringEvent, MonitoringEventPayload};

struct WorkerCountRecord {
    time: SystemTime,
    count: usize,
}

/// Stores information about the number of connected workers in time in an efficient way.
#[derive(Default)]
pub struct WorkerCountTimeline {
    timeline: Vec<WorkerCountRecord>,
}

impl WorkerCountTimeline {
    /// Assumes that `events` are sorted by time.
    pub fn handle_new_events(&mut self, events: &[MonitoringEvent]) {
        let mut last_count = self.timeline.last().map(|record| record.count).unwrap_or(0);

        for event in events {
            match event.payload {
                MonitoringEventPayload::WorkerConnected(_, _) => {
                    last_count += 1;
                    self.timeline.push(WorkerCountRecord {
                        time: event.time,
                        count: last_count,
                    });
                }
                MonitoringEventPayload::WorkerLost(_, _) => {
                    last_count = last_count.saturating_sub(1);
                    self.timeline.push(WorkerCountRecord {
                        time: event.time,
                        count: last_count,
                    });
                }
                _ => {}
            }
        }
    }

    pub fn get_worker_count(&self, time: SystemTime) -> usize {
        let index = self.timeline.partition_point(|w| w.time < time);
        if index == 0 {
            0
        } else {
            self.timeline[index - 1].count
        }
    }
}
