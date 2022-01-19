use crate::WorkerId;
use std::time::SystemTime;
use tako::messages::common::WorkerConfiguration;
use tako::messages::gateway::LostWorkerReason;
use tako::server::monitoring::{MonitoringEvent, MonitoringEventPayload};

#[derive(Clone)]
pub struct WorkerConnectionInfo {
    worker_id: WorkerId,
    connection_time: SystemTime,
    worker_info: WorkerConfiguration,

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
                MonitoringEventPayload::WorkerConnected(id, info) => {
                    self.worker_connection_timeline.push(WorkerConnectionInfo {
                        worker_id: *id,
                        connection_time: event.time,
                        worker_info: *info.clone(),
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

    pub fn get_worker_info_for(&self, worker_id: &WorkerId) -> Option<&WorkerConfiguration> {
        return self
            .worker_connection_timeline
            .iter()
            .find(|info| info.worker_id == *worker_id)
            .map(|info| &info.worker_info);
    }

    pub fn get_connected_worker_ids(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = WorkerId> + '_ {
        self.worker_connection_timeline
            .iter()
            .filter(move |wkr| {
                let has_started = wkr.connection_time <= time;
                let has_finished = match wkr.lost_info {
                    Some((lost_time, _)) => lost_time <= time,
                    None => false,
                };
                has_started && !has_finished
            })
            .map(|x| x.worker_id)
    }
}
