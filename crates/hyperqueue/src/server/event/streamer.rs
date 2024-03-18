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

#[derive(Clone)]
pub struct EventStreamer {
    stream_sender: Option<EventStreamSender>,
}

impl EventStreamer {
    pub fn new(stream_sender: Option<EventStreamSender>) -> Self {
        Self { stream_sender }
    }
    //
    // /// Returns all events that have event ID larger than `id`.
    // /// There is no guarantee on the order of the events, if you want any specific order, you have
    // /// to sort them.
    // pub fn get_events_after(&self, id: EventId) -> impl Iterator<Item = &Event> {
    //     todo!()
    //     /*self.event_queue
    //     .iter()
    //     .rev()
    //     .take_while(move |event| event.id > id)*/
    // }

    pub fn on_worker_added(&self, id: WorkerId, configuration: WorkerConfiguration) {
        self.send_event(EventPayload::WorkerConnected(id, Box::new(configuration)));
    }

    pub fn on_worker_lost(&self, id: WorkerId, reason: LostWorkerReason) {
        self.send_event(EventPayload::WorkerLost(id, reason));
    }

    #[inline]
    pub fn on_overview_received(&self, worker_overview: WorkerOverview) {
        self.send_event(EventPayload::WorkerOverviewReceived(worker_overview));
    }

    pub fn on_job_submitted_full(
        &self,
        job_id: JobId,
        job_desc: &JobDescription,
    ) -> crate::Result<()> {
        if self.stream_sender.is_none() {
            // Skip serialization if there is no streaming end
            return Ok(());
        }
        self.send_event(EventPayload::JobCreatedFull(
            job_id,
            bincode_config().serialize(job_desc)?,
        ));
        Ok(())
    }

    #[inline]
    pub fn on_job_completed(&self, job_id: JobId) {
        self.send_event(EventPayload::JobCompleted(job_id));
    }

    #[inline]
    pub fn on_task_started(
        &self,
        job_id: JobId,
        task_id: JobTaskId,
        instance_id: InstanceId,
        worker_ids: SmallVec<[WorkerId; 1]>,
    ) {
        self.send_event(EventPayload::TaskStarted {
            job_id,
            task_id,
            instance_id,
            workers: worker_ids,
        });
    }

    #[inline]
    pub fn on_task_finished(&self, job_id: JobId, task_id: JobTaskId) {
        self.send_event(EventPayload::TaskFinished { job_id, task_id });
    }

    pub fn on_task_canceled(&self, job_id: JobId, task_id: JobTaskId) {
        self.send_event(EventPayload::TaskCanceled { job_id, task_id });
    }

    #[inline]
    pub fn on_task_failed(&self, job_id: JobId, task_id: JobTaskId, error: String) {
        self.send_event(EventPayload::TaskFailed {
            job_id,
            task_id,
            error,
        });
    }

    pub fn on_allocation_queue_created(&self, id: QueueId, parameters: AllocationQueueParams) {
        self.send_event(EventPayload::AllocationQueueCreated(
            id,
            Box::new(parameters),
        ))
    }

    pub fn on_allocation_queue_removed(&self, id: QueueId) {
        self.send_event(EventPayload::AllocationQueueRemoved(id))
    }

    pub fn on_allocation_queued(
        &self,
        queue_id: QueueId,
        allocation_id: AllocationId,
        worker_count: u64,
    ) {
        self.send_event(EventPayload::AllocationQueued {
            queue_id,
            allocation_id,
            worker_count,
        })
    }

    pub fn on_allocation_started(&self, queue_id: QueueId, allocation_id: AllocationId) {
        self.send_event(EventPayload::AllocationStarted(queue_id, allocation_id));
    }

    pub fn on_allocation_finished(&self, queue_id: QueueId, allocation_id: AllocationId) {
        self.send_event(EventPayload::AllocationFinished(queue_id, allocation_id));
    }

    fn send_event(&self, payload: EventPayload) {
        if let Some(ref streamer) = self.stream_sender {
            if streamer
                .send(Event {
                    time: Utc::now(),
                    payload,
                })
                .is_err()
            {
                log::error!("Event streaming queue has been closed.");
            }
        }
    }

    pub fn on_server_stop(&self) {
        self.send_event(EventPayload::ServerStop);
    }
}
