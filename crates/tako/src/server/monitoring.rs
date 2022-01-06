use crate::messages::gateway::LostWorkerReason;
use crate::messages::worker::WorkerOverview;
use crate::{static_assert_size, WorkerId};

use crate::messages::common::WorkerConfiguration;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::SystemTime;

pub struct EventStorage {
    event_store_size: usize,
    event_queue: VecDeque<MonitoringEvent>,
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
    OverviewUpdate(WorkerOverview),
}

// Keep the size of the event structure in check
static_assert_size!(MonitoringEventPayload, 160);

impl Default for EventStorage {
    fn default() -> Self {
        Self {
            event_store_size: 1_000_000,
            event_queue: Default::default(),
            last_event_id: 0,
        }
    }
}

impl EventStorage {
    pub fn new(event_store_size: usize) -> Self {
        Self {
            event_store_size,
            event_queue: VecDeque::new(),
            last_event_id: 0,
        }
    }

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
    pub fn on_overview_received(&mut self, worker_overview: WorkerOverview) {
        self.insert_event(MonitoringEventPayload::OverviewUpdate(worker_overview));
    }

    fn insert_event(&mut self, payload: MonitoringEventPayload) {
        self.last_event_id += 1;
        self.event_queue.push_back(MonitoringEvent {
            payload,
            id: self.last_event_id,
            time: SystemTime::now(),
        });
        if self.event_queue.len() > self.event_store_size {
            self.event_queue.pop_front();
        }
    }
}
