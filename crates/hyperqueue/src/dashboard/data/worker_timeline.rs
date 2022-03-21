use crate::server::event::events::MonitoringEventPayload;
use crate::server::event::MonitoringEvent;
use crate::WorkerId;
use std::time::SystemTime;
use tako::messages::common::WorkerConfiguration;
use tako::messages::gateway::LostWorkerReason;
use tako::messages::worker::WorkerOverview;

#[derive(Clone)]
pub struct WorkerHistory {
    worker_id: WorkerId,
    connection_time: SystemTime,
    worker_info: WorkerConfiguration,
    worker_overviews: Vec<WorkerOverview>,

    lost_info: Option<(SystemTime, LostWorkerReason)>,
}

impl WorkerHistory {
    pub fn set_loss_details(&mut self, loss_time: &SystemTime, loss_reason: &LostWorkerReason) {
        self.lost_info = Some((*loss_time, loss_reason.clone()));
    }
}

/// Stores information about the workers at different times
#[derive(Default)]
pub struct WorkerTimeline {
    worker_timeline: Vec<WorkerHistory>,
}

impl WorkerTimeline {
    /// Assumes that `events` are sorted by time.
    pub fn handle_new_events(&mut self, events: &[MonitoringEvent]) {
        for event in events {
            match &event.payload {
                MonitoringEventPayload::WorkerConnected(id, info) => {
                    self.worker_timeline.push(WorkerHistory {
                        worker_id: *id,
                        connection_time: event.time,
                        worker_info: *info.clone(),
                        worker_overviews: vec![],
                        lost_info: None,
                    });
                }
                MonitoringEventPayload::WorkerLost(lost_id, reason) => {
                    for info in &mut self.worker_timeline {
                        if info.worker_id == *lost_id {
                            info.set_loss_details(&event.time, reason);
                        }
                    }
                }
                MonitoringEventPayload::WorkerOverviewReceived(overview) => {
                    if let Some(worker_history) = self
                        .worker_timeline
                        .iter_mut()
                        .find(|history| history.worker_id == overview.id)
                    {
                        worker_history.worker_overviews.push(overview.clone());
                    }
                }
                _ => {}
            }
        }
    }

    pub fn get_worker_info_for(&self, worker_id: &WorkerId) -> Option<&WorkerConfiguration> {
        return self
            .worker_timeline
            .iter()
            .find(|info| info.worker_id == *worker_id)
            .map(|info| &info.worker_info);
    }

    pub fn get_connected_worker_ids(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = WorkerId> + '_ {
        self.worker_timeline
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

    pub fn get_worker_overview_at(
        &self,
        worker_id: WorkerId,
        time: SystemTime,
    ) -> Option<&WorkerOverview> {
        self.get_last_received_overviews(time)
            .into_iter()
            .find(|overview| overview.id == worker_id)
    }

    pub fn get_last_received_overviews(&self, time: SystemTime) -> Vec<&WorkerOverview> {
        self.get_connected_worker_ids(time)
            .filter_map(|wkr_id| self.get_last_overview_received_from(wkr_id))
            .collect()
    }

    fn get_last_overview_received_from(&self, worker_id: WorkerId) -> Option<&WorkerOverview> {
        self.worker_timeline
            .iter()
            .find(|history| history.worker_id == worker_id)
            .and_then(|history| history.worker_overviews.last())
    }
}
