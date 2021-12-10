use crate::messages::gateway::{CollectedOverview, LostWorkerReason};
use crate::messages::worker::WorkerOverview;
use crate::WorkerId;

use crate::messages::common::WorkerConfiguration;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Default)]
pub struct EventStorage {
    event_queue: Vec<MonitoringEvent>,
    last_event_id: u32,
}

pub type MonitoringEventId = u32;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MonitoringEvent {
    pub id: MonitoringEventId,
    pub time: SystemTime,
    pub payload: MonitoringEventPayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MonitoringEventPayload {
    WorkerConnected(WorkerId, Box<WorkerConfiguration>),
    WorkerLost(WorkerId, LostWorkerReason),
    OverviewUpdate(CollectedOverview),
}

// Keep the size of the event structure in check
const _: () = assert!(std::mem::size_of::<MonitoringEventPayload>() == 32);

impl EventStorage {
    /// Returns all events that have event ID larger than `id`.
    /// There is no guarantee on the order of the events, if you want any specific order, you have
    /// to sort them.
    pub fn get_events_after(
        &self,
        id: MonitoringEventId,
    ) -> impl Iterator<Item = &MonitoringEvent> {
        self.event_queue
            .iter()
            .rev()
            .take_while(move |event| event.id > id)
    }

    #[inline]
    pub fn on_worker_added(&mut self, id: WorkerId, configuration: WorkerConfiguration) {
        self.insert_event(MonitoringEventPayload::WorkerConnected(
            id,
            Box::new(configuration),
        ));
    }

    #[inline]
    pub fn on_remove_worker(&mut self, id: WorkerId, reason: LostWorkerReason) {
        self.insert_event(MonitoringEventPayload::WorkerLost(id, reason));
    }

    #[inline]
    pub fn on_overview_received(&mut self, worker_overviews: Vec<WorkerOverview>) {
        self.insert_event(MonitoringEventPayload::OverviewUpdate(CollectedOverview {
            worker_overviews,
        }));
    }

    fn insert_event(&mut self, payload: MonitoringEventPayload) {
        self.last_event_id += 1;
        self.event_queue.push(MonitoringEvent {
            payload,
            id: self.last_event_id,
            time: SystemTime::now(),
        })
    }
}
