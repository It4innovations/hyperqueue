use crate::common::serialization::Serialized;
use crate::server::autoalloc::{AllocationId, QueueId};
use crate::server::event::Event;
use crate::server::event::journal::{EventStreamMessage, EventStreamSender};
use crate::server::event::payload::EventPayload;
use crate::transfer::messages::{AllocationQueueParams, JobDescription, SubmitRequest};
use crate::{JobId, JobTaskId, WorkerId};
use chrono::Utc;
use smallvec::SmallVec;
use tako::gateway::LostWorkerReason;
use tako::worker::{WorkerConfiguration, WorkerOverview};
use tako::{InstanceId, Set, WrappedRcRefCell};
use tokio::sync::{mpsc, oneshot};

struct Inner {
    storage_sender: Option<EventStreamSender>,
    client_listeners: Vec<(mpsc::UnboundedSender<Event>, u32)>,
}

#[derive(Clone)]
pub struct EventStreamer {
    inner: WrappedRcRefCell<Inner>,
}

impl EventStreamer {
    pub fn new(stream_sender: Option<EventStreamSender>) -> Self {
        Self {
            inner: WrappedRcRefCell::wrap(Inner {
                storage_sender: stream_sender,
                client_listeners: Vec::new(),
            }),
        }
    }

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

    pub fn on_job_opened(&self, job_id: JobId, job_desc: JobDescription) {
        self.send_event(EventPayload::JobOpen(job_id, job_desc));
    }

    pub fn on_job_submitted(
        &self,
        job_id: JobId,
        submit_request: &SubmitRequest,
    ) -> crate::Result<()> {
        {
            let inner = self.inner.get();
            if inner.storage_sender.is_none() && inner.client_listeners.is_empty() {
                // Skip serialization if there is no streaming end
                return Ok(());
            }
        }
        self.send_event(EventPayload::Submit {
            job_id,
            closed_job: submit_request.job_id.is_none(),
            serialized_desc: Serialized::new(submit_request)?,
        });
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
        let mut inner = self.inner.get_mut();
        if inner.storage_sender.is_none() && inner.client_listeners.is_empty() {
            return;
        }
        let event = Event {
            time: Utc::now(),
            payload,
        };
        inner
            .client_listeners
            .retain(|(listener, _)| listener.send(event.clone()).is_ok());
        if let Some(ref streamer) = inner.storage_sender {
            if streamer.send(EventStreamMessage::Event(event)).is_err() {
                log::error!("Event streaming queue has been closed.");
            }
        }
    }

    pub fn on_server_stop(&self) {
        self.send_event(EventPayload::ServerStop);
    }

    pub fn on_server_start(&self, server_uid: &str) {
        self.send_event(EventPayload::ServerStart {
            server_uid: server_uid.to_string(),
        });
    }

    pub fn register_listener(
        &self,
        history_sender: mpsc::UnboundedSender<Event>,
        current_sender: mpsc::UnboundedSender<Event>,
    ) -> u32 {
        let mut inner = self.inner.get_mut();
        let listener_id = inner
            .client_listeners
            .iter()
            .map(|x| x.1)
            .max()
            .unwrap_or(0)
            + 1;
        inner.client_listeners.push((current_sender, listener_id));
        if let Some(ref streamer) = inner.storage_sender {
            if streamer
                .send(EventStreamMessage::RegisterListener(history_sender))
                .is_err()
            {
                log::error!("Event streaming queue has been closed.");
            }
        }
        listener_id
    }

    pub fn unregister_listener(&self, listener_id: u32) {
        let mut inner = self.inner.get_mut();
        let p = inner
            .client_listeners
            .iter()
            .position(|(_, id)| *id == listener_id)
            .unwrap();
        inner.client_listeners.remove(p);
    }

    pub fn flush_journal(&self) -> Option<oneshot::Receiver<()>> {
        let inner = self.inner.get();
        if let Some(ref streamer) = inner.storage_sender {
            let (sender, receiver) = oneshot::channel();
            if streamer
                .send(EventStreamMessage::FlushJournal(sender))
                .is_err()
            {
                log::error!("Event streaming queue has been closed.");
            }
            Some(receiver)
        } else {
            None
        }
    }

    pub fn prune_journal(
        &self,
        live_jobs: Set<JobId>,
        live_workers: Set<WorkerId>,
    ) -> Option<oneshot::Receiver<()>> {
        let inner = self.inner.get();
        if let Some(ref streamer) = inner.storage_sender {
            let (sender, receiver) = oneshot::channel();
            if streamer
                .send(EventStreamMessage::PruneJournal {
                    live_jobs,
                    live_workers,
                    callback: sender,
                })
                .is_err()
            {
                log::error!("Event streaming queue has been closed.");
            }
            Some(receiver)
        } else {
            None
        }
    }
}
