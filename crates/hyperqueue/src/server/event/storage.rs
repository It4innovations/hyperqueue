use crate::server::autoalloc::DescriptorId;
use crate::server::event::events::MonitoringEventPayload;
use crate::server::event::log::EventStreamSender;
use crate::server::event::{MonitoringEvent, MonitoringEventId};
use crate::transfer::messages::AllocationQueueParams;
use crate::WorkerId;
use std::collections::VecDeque;
use std::time::SystemTime;
use tako::messages::common::WorkerConfiguration;
use tako::messages::gateway::LostWorkerReason;
use tako::messages::worker::WorkerOverview;

pub struct EventStorage {
    event_store_size: usize,
    event_queue: VecDeque<MonitoringEvent>,
    last_event_id: u32,
    stream_sender: Option<EventStreamSender>,
}

impl Default for EventStorage {
    fn default() -> Self {
        Self {
            event_store_size: 1_000_000,
            event_queue: Default::default(),
            last_event_id: 0,
            stream_sender: None,
        }
    }
}

impl EventStorage {
    pub fn new(event_store_size: usize, stream_sender: Option<EventStreamSender>) -> Self {
        Self {
            event_store_size,
            event_queue: VecDeque::new(),
            last_event_id: 0,
            stream_sender,
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

    pub fn on_worker_added(&mut self, id: WorkerId, configuration: WorkerConfiguration) {
        self.insert_event(MonitoringEventPayload::WorkerConnected(
            id,
            Box::new(configuration),
        ));
    }

    pub fn on_worker_lost(&mut self, id: WorkerId, reason: LostWorkerReason) {
        self.insert_event(MonitoringEventPayload::WorkerLost(id, reason));
    }

    #[inline]
    pub fn on_overview_received(&mut self, worker_overview: WorkerOverview) {
        self.insert_event(MonitoringEventPayload::WorkerOverviewReceived(
            worker_overview,
        ));
    }

    pub fn on_allocation_queue_created(
        &mut self,
        id: DescriptorId,
        parameters: AllocationQueueParams,
    ) {
        self.insert_event(MonitoringEventPayload::AllocationQueueCreated(
            id,
            Box::new(parameters),
        ))
    }

    pub fn on_allocation_queue_removed(&mut self, id: DescriptorId) {
        self.insert_event(MonitoringEventPayload::AllocationQueueRemoved(id))
    }

    fn insert_event(&mut self, payload: MonitoringEventPayload) {
        self.last_event_id += 1;

        let event = MonitoringEvent {
            payload,
            id: self.last_event_id,
            time: SystemTime::now(),
        };
        self.stream_event(&event);
        self.event_queue.push_back(event);

        if self.event_queue.len() > self.event_store_size {
            self.event_queue.pop_front();
        }
    }

    fn stream_event(&mut self, event: &MonitoringEvent) {
        if let Some(ref streamer) = self.stream_sender {
            if streamer.send(event.clone()).is_err() {
                log::error!("Event streaming queue has been closed.");
                self.stream_sender = None;
            }
        }
    }
}
