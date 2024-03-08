use crate::server::autoalloc::{AllocationId, QueueId};
use crate::server::event::log::EventStreamSender;
use crate::server::event::payload::EventPayload;
use crate::server::event::{bincode_config, Event, EventId};
use crate::transfer::messages::{AllocationQueueParams, JobDescription};
use crate::{JobId, JobTaskId, TakoTaskId, WorkerId};
use bincode::Options;
use chrono::{DateTime, Utc};
use smallvec::SmallVec;
use std::collections::vec_deque::VecDeque;
use std::rc::Rc;
use std::time::SystemTime;
use tako::gateway::LostWorkerReason;
use tako::worker::{WorkerConfiguration, WorkerOverview};
use tako::{InstanceId, TaskId};

pub struct EventStorage {
    event_store_size: usize,
    event_queue: VecDeque<Event>,
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
    pub fn get_events_after(&self, id: EventId) -> impl Iterator<Item = &Event> {
        self.event_queue
            .iter()
            .rev()
            .take_while(move |event| event.id > id)
    }

    pub fn on_worker_added(&mut self, id: WorkerId, configuration: WorkerConfiguration) {
        self.insert_event(EventPayload::WorkerConnected(id, Box::new(configuration)));
    }

    pub fn on_worker_lost(&mut self, id: WorkerId, reason: LostWorkerReason) {
        self.insert_event(EventPayload::WorkerLost(id, reason));
    }

    #[inline]
    pub fn on_overview_received(&mut self, worker_overview: WorkerOverview) {
        self.insert_event(EventPayload::WorkerOverviewReceived(worker_overview));
    }

    pub fn on_job_submitted_full(
        &mut self,
        job_id: JobId,
        job_desc: &JobDescription,
    ) -> crate::Result<()> {
        self.insert_event(EventPayload::JobCreatedFull(
            job_id,
            bincode_config().serialize(job_desc)?,
        ));
        Ok(())
    }

    // pub fn on_job_submitted_short(&mut self, job_id: JobId, job_desc: Rc<JobDescription>) {
    //     self.insert_event(EventPayload::JobCreatedShort(job_id, job_desc));
    // }

    #[inline]
    pub fn on_job_completed(&mut self, job_id: JobId) {
        self.insert_event(EventPayload::JobCompleted(job_id));
    }

    #[inline]
    pub fn on_task_started(
        &mut self,
        job_id: JobId,
        task_id: JobTaskId,
        instance_id: InstanceId,
        worker_ids: SmallVec<[WorkerId; 1]>,
    ) {
        self.insert_event(EventPayload::TaskStarted {
            job_id,
            task_id,
            instance_id,
            workers: worker_ids,
        });
    }

    #[inline]
    pub fn on_task_finished(&mut self, job_id: JobId, task_id: JobTaskId) {
        self.insert_event(EventPayload::TaskFinished { job_id, task_id });
    }

    #[inline]
    pub fn on_task_failed(&mut self, job_id: JobId, task_id: JobTaskId, error: String) {
        self.insert_event(EventPayload::TaskFailed {
            job_id,
            task_id,
            error,
        });
    }

    pub fn on_allocation_queue_created(&mut self, id: QueueId, parameters: AllocationQueueParams) {
        self.insert_event(EventPayload::AllocationQueueCreated(
            id,
            Box::new(parameters),
        ))
    }

    pub fn on_allocation_queue_removed(&mut self, id: QueueId) {
        self.insert_event(EventPayload::AllocationQueueRemoved(id))
    }

    pub fn on_allocation_queued(
        &mut self,
        queue_id: QueueId,
        allocation_id: AllocationId,
        worker_count: u64,
    ) {
        self.insert_event(EventPayload::AllocationQueued {
            queue_id,
            allocation_id,
            worker_count,
        })
    }

    pub fn on_allocation_started(&mut self, queue_id: QueueId, allocation_id: AllocationId) {
        self.insert_event(EventPayload::AllocationStarted(queue_id, allocation_id));
    }

    pub fn on_allocation_finished(&mut self, queue_id: QueueId, allocation_id: AllocationId) {
        self.insert_event(EventPayload::AllocationFinished(queue_id, allocation_id));
    }

    fn insert_event(&mut self, payload: EventPayload) {
        self.last_event_id += 1;

        let event = Event {
            payload,
            id: self.last_event_id,
            time: Utc::now(),
        };
        self.stream_event(&event);
        self.event_queue.push_back(event);

        if self.event_queue.len() > self.event_store_size {
            self.event_queue.pop_front();
        }
    }

    fn stream_event(&mut self, event: &Event) {
        if let Some(ref streamer) = self.stream_sender {
            if streamer.send(event.clone()).is_err() {
                log::error!("Event streaming queue has been closed.");
                self.stream_sender = None;
            }
        }
    }

    pub fn close_stream(&mut self) {
        self.stream_sender = None;
    }
}
