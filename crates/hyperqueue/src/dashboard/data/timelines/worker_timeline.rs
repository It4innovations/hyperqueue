use crate::WorkerId;
use crate::dashboard::data::Time;
use crate::dashboard::data::time_based_vec::{ItemWithTime, TimeBasedVec};
use crate::dashboard::data::time_interval::TimeRange;
use crate::server::event::Event;
use crate::server::event::payload::EventPayload;
use std::time::SystemTime;
use tako::Map;
use tako::gateway::LostWorkerReason;
use tako::worker::{WorkerConfiguration, WorkerOverview};

#[derive(Clone)]
pub struct WorkerDisconnectInfo {
    pub reason: LostWorkerReason,
    pub time: Time,
}

pub enum WorkerStatus {
    Connected,
    Disconnected(WorkerDisconnectInfo),
}

pub struct WorkerRecord {
    connection_time: SystemTime,
    worker_config: WorkerConfiguration,
    worker_overviews: TimeBasedVec<WorkerOverview>,

    disconnect_info: Option<WorkerDisconnectInfo>,
}

impl WorkerRecord {
    pub fn set_loss_details(&mut self, loss_time: SystemTime, loss_reason: LostWorkerReason) {
        self.disconnect_info = Some(WorkerDisconnectInfo {
            reason: loss_reason,
            time: loss_time,
        });
    }
}

/// Stores information about the workers at different times
#[derive(Default)]
pub struct WorkerTimeline {
    workers: Map<WorkerId, WorkerRecord>,
}

impl WorkerTimeline {
    /// Assumes that `events` are sorted by time.
    pub fn handle_new_events(&mut self, events: &[Event]) {
        for event in events {
            match &event.payload {
                EventPayload::WorkerConnected(id, info) => {
                    self.workers.insert(
                        *id,
                        WorkerRecord {
                            connection_time: event.time.into(),
                            worker_config: *info.clone(),
                            worker_overviews: Default::default(),
                            disconnect_info: None,
                        },
                    );
                }
                EventPayload::WorkerLost(lost_id, reason) => {
                    if let Some(worker) = self.workers.get_mut(lost_id) {
                        worker.set_loss_details(event.time.into(), reason.clone());
                    }
                }
                EventPayload::WorkerOverviewReceived(overview) => {
                    if let Some(worker) = self.workers.get_mut(&overview.id) {
                        worker
                            .worker_overviews
                            .push(event.time.into(), overview.clone());
                    }
                }
                _ => {}
            }
        }
    }

    pub fn query_worker_config_for(&self, worker_id: WorkerId) -> Option<&WorkerConfiguration> {
        self.workers.get(&worker_id).map(|w| &w.worker_config)
    }

    pub fn query_connected_worker_ids_at(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = WorkerId> + '_ {
        self.query_known_worker_ids_at(time)
            .filter_map(|(id, status)| match status {
                WorkerStatus::Connected => Some(id),
                WorkerStatus::Disconnected(_) => None,
            })
    }

    pub fn query_known_worker_ids_at(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = (WorkerId, WorkerStatus)> + '_ {
        self.workers.iter().filter_map(move |(worker_id, worker)| {
            let has_started = worker.connection_time <= time;
            let disconnect_info = worker.disconnect_info.as_ref().and_then(|info| {
                if info.time <= time {
                    Some(info.clone())
                } else {
                    None
                }
            });
            let status = if has_started {
                if let Some(disconnect_info) = disconnect_info {
                    WorkerStatus::Disconnected(disconnect_info)
                } else {
                    WorkerStatus::Connected
                }
            } else {
                return None;
            };
            Some((*worker_id, status))
        })
    }

    pub fn query_worker_overview_at(
        &self,
        worker_id: WorkerId,
        time: SystemTime,
    ) -> Option<&ItemWithTime<WorkerOverview>> {
        self.workers
            .get(&worker_id)
            .and_then(|worker| worker.worker_overviews.get_most_recent_at(time))
    }

    pub fn query_worker_overviews_at(
        &self,
        worker_id: WorkerId,
        range: TimeRange,
    ) -> Option<&[ItemWithTime<WorkerOverview>]> {
        self.workers
            .get(&worker_id)
            .map(|worker| worker.worker_overviews.get_time_range(range))
    }
}
