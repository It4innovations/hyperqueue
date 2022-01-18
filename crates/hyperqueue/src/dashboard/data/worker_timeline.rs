use crate::WorkerId;
use std::time::SystemTime;
use tako::messages::gateway::LostWorkerReason;
use tako::server::monitoring::{MonitoringEvent, MonitoringEventPayload};

#[derive(Clone)]
pub struct WorkerConnectionInfo {
    worker_id: WorkerId,
    connection_time: SystemTime,

    lost_info: Option<(SystemTime, LostWorkerReason)>,
}

impl WorkerConnectionInfo {
    pub fn set_loss_details(&mut self, loss_time: &SystemTime, loss_reason: &LostWorkerReason) {
        self.lost_info = Some((*loss_time, loss_reason.clone()));
    }
}

/// Stores information about the workers at different times
#[derive(Default)]
pub struct WorkerTimeline {
    worker_connection_timeline: Vec<WorkerConnectionInfo>,
}

impl WorkerTimeline {
    /// Assumes that `events` are sorted by time.
    pub fn handle_new_events(&mut self, events: &[MonitoringEvent]) {
        for event in events {
            match &event.payload {
                MonitoringEventPayload::WorkerConnected(id, _) => {
                    self.worker_connection_timeline.push(WorkerConnectionInfo {
                        worker_id: *id,
                        connection_time: event.time,
                        lost_info: None,
                    });
                }
                MonitoringEventPayload::WorkerLost(lost_id, reason) => {
                    for info in &mut self.worker_connection_timeline {
                        if info.worker_id == *lost_id {
                            info.set_loss_details(&event.time, reason);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub fn get_worker_count(&self, time: SystemTime) -> usize {
        self.get_connected_worker_ids(time).len()
    }

    pub fn get_connected_worker_ids(&self, time: SystemTime) -> Vec<WorkerId> {
        self.worker_connection_timeline
            .iter()
            .filter(|wkr| {
                let has_started = wkr.connection_time <= time;
                let has_finished = match wkr.lost_info {
                    Some((lost_time, _)) => lost_time <= time,
                    None => false,
                };
                has_started && !has_finished
            })
            .map(|x| x.worker_id)
            .collect()
    }
}
