use crate::common::serialization::Serialized;
use crate::server::autoalloc::{AllocationId, QueueId, QueueParameters};
use crate::server::event::Event;
use crate::server::event::journal::{EventStreamMessage, EventStreamSender};
use crate::server::event::payload::{EventPayload, TaskNotification};
use crate::transfer::messages::{JobDescription, SubmitRequest};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tako::gateway::LostWorkerReason;
use tako::worker::{WorkerConfiguration, WorkerOverview};
use tako::{InstanceId, JobId, ResourceVariantId, Set, TaskId, WorkerId, WrappedRcRefCell};
use tokio::sync::{mpsc, oneshot};

struct EventListener {
    filter: EventFilter,
    sender: mpsc::UnboundedSender<Event>,
    id: u32,
}

struct Inner {
    storage_sender: Option<EventStreamSender>,
    client_listeners: Vec<EventListener>,
}

/// How should events be forwarded.
enum ForwardMode {
    /// Only stream the event to clients.
    Stream,
    /// Stream the event to clients and persist it.
    StreamAndPersist,
}

bitflags::bitflags! {
    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub struct EventFilterFlags: u32 {
        const JOB_EVENTS = 0b00000001;
        const TASK_EVENTS = 0b00000010;
        const WORKER_EVENTS = 0b00000100;
        const ALLOCATION_EVENTS = 0b00001000;
        const NOTIFY_EVENTS = 0b00010000;

        const ALL_EVENTS = 0b11111111;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventFilter {
    jobs: Option<Set<JobId>>,
    flags: EventFilterFlags,
}

impl EventFilter {
    pub fn new(jobs: Option<Set<JobId>>, flags: EventFilterFlags) -> Self {
        EventFilter { jobs, flags }
    }

    pub fn job_events(jobs: Set<JobId>) -> Self {
        let mut flags = EventFilterFlags::empty();
        flags.insert(EventFilterFlags::JOB_EVENTS);
        EventFilter {
            jobs: Some(jobs),
            flags,
        }
    }

    pub fn all_events() -> Self {
        EventFilter {
            jobs: None,
            flags: EventFilterFlags::all(),
        }
    }

    pub fn is_filtering_jobs(&self) -> bool {
        self.jobs.is_some()
    }

    pub fn set_jobs(&mut self, jobs: Set<JobId>) {
        self.jobs = Some(jobs);
    }

    pub fn check(&self, payload: &EventPayload) -> bool {
        match payload {
            EventPayload::WorkerConnected(_, _)
            | EventPayload::WorkerLost(_, _)
            | EventPayload::WorkerOverviewReceived(_) => {
                self.flags.contains(EventFilterFlags::WORKER_EVENTS)
            }
            EventPayload::Submit { job_id, .. }
            | EventPayload::JobCompleted(job_id)
            | EventPayload::JobOpen(job_id, _)
            | EventPayload::JobClose(job_id)
            | EventPayload::JobIdle(job_id) => {
                if !self.flags.contains(EventFilterFlags::JOB_EVENTS) {
                    false
                } else {
                    self.jobs
                        .as_ref()
                        .map(|jobs| jobs.contains(job_id))
                        .unwrap_or(true)
                }
            }
            EventPayload::TaskStarted { task_id, .. }
            | EventPayload::TaskFinished { task_id }
            | EventPayload::TaskFailed { task_id, .. } => {
                if !self.flags.contains(EventFilterFlags::TASK_EVENTS) {
                    false
                } else {
                    self.jobs
                        .as_ref()
                        .map(|jobs| jobs.contains(&task_id.job_id()))
                        .unwrap_or(true)
                }
            }
            EventPayload::TasksCanceled { task_ids } => {
                if !self.flags.contains(EventFilterFlags::TASK_EVENTS) {
                    false
                } else {
                    self.jobs
                        .as_ref()
                        .map(|jobs| {
                            task_ids
                                .iter()
                                .any(|task_id| jobs.contains(&task_id.job_id()))
                        })
                        .unwrap_or(true)
                }
            }
            EventPayload::AllocationQueueCreated(_, _)
            | EventPayload::AllocationQueueRemoved(_)
            | EventPayload::AllocationQueued { .. }
            | EventPayload::AllocationStarted(_, _)
            | EventPayload::AllocationFinished(_, _) => {
                self.flags.contains(EventFilterFlags::ALLOCATION_EVENTS)
            }
            EventPayload::ServerStart { .. } | EventPayload::ServerStop => true,
            EventPayload::TaskNotify(notify) => {
                if !self.flags.contains(EventFilterFlags::NOTIFY_EVENTS) {
                    false
                } else {
                    self.jobs
                        .as_ref()
                        .map(|jobs| jobs.contains(&notify.task_id.job_id()))
                        .unwrap_or(true)
                }
            }
        }
    }
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
        self.send_event(
            EventPayload::WorkerConnected(id, Box::new(configuration)),
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_worker_lost(&self, id: WorkerId, reason: LostWorkerReason) {
        self.send_event(
            EventPayload::WorkerLost(id, reason),
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    #[inline]
    pub fn on_overview_received(&self, worker_overview: Box<WorkerOverview>, persist_event: bool) {
        self.send_event(
            EventPayload::WorkerOverviewReceived(worker_overview),
            None,
            if persist_event {
                ForwardMode::StreamAndPersist
            } else {
                ForwardMode::Stream
            },
        )
    }
    pub fn on_job_opened(&self, job_id: JobId, job_desc: JobDescription) {
        self.send_event(
            EventPayload::JobOpen(job_id, job_desc),
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_job_closed(&self, job_id: JobId) {
        self.send_event(
            EventPayload::JobClose(job_id),
            None,
            ForwardMode::StreamAndPersist,
        );
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
        self.send_event(
            EventPayload::Submit {
                job_id,
                closed_job: submit_request.job_id.is_none(),
                serialized_desc: Serialized::new(submit_request)?,
            },
            None,
            ForwardMode::StreamAndPersist,
        );
        Ok(())
    }

    #[inline]
    pub fn on_job_completed(&self, job_id: JobId, now: DateTime<Utc>) {
        self.send_event(
            EventPayload::JobCompleted(job_id),
            Some(now),
            ForwardMode::StreamAndPersist,
        );
    }

    #[inline]
    pub fn on_task_started(
        &self,
        task_id: TaskId,
        instance_id: InstanceId,
        worker_ids: SmallVec<[WorkerId; 1]>,
        resource_variant: ResourceVariantId,
        now: DateTime<Utc>,
    ) {
        self.send_event(
            EventPayload::TaskStarted {
                task_id,
                instance_id,
                worker_ids,
                rv_id: resource_variant,
            },
            Some(now),
            ForwardMode::StreamAndPersist,
        );
    }

    #[inline]
    pub fn on_task_finished(&self, task_id: TaskId, now: DateTime<Utc>) {
        self.send_event(
            EventPayload::TaskFinished { task_id },
            Some(now),
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_task_canceled(&self, task_ids: Vec<TaskId>, now: DateTime<Utc>) {
        self.send_event(
            EventPayload::TasksCanceled { task_ids },
            Some(now),
            ForwardMode::StreamAndPersist,
        );
    }

    #[inline]
    pub fn on_task_failed(&self, task_id: TaskId, error: String, now: DateTime<Utc>) {
        self.send_event(
            EventPayload::TaskFailed { task_id, error },
            Some(now),
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_allocation_queue_created(&self, id: QueueId, parameters: QueueParameters) {
        self.send_event(
            EventPayload::AllocationQueueCreated(id, Box::new(parameters)),
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_allocation_queue_removed(&self, id: QueueId) {
        self.send_event(
            EventPayload::AllocationQueueRemoved(id),
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_allocation_queued(
        &self,
        queue_id: QueueId,
        allocation_id: AllocationId,
        worker_count: u64,
    ) {
        self.send_event(
            EventPayload::AllocationQueued {
                queue_id,
                allocation_id,
                worker_count,
            },
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_allocation_started(&self, queue_id: QueueId, allocation_id: AllocationId) {
        self.send_event(
            EventPayload::AllocationStarted(queue_id, allocation_id),
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_allocation_finished(&self, queue_id: QueueId, allocation_id: AllocationId) {
        self.send_event(
            EventPayload::AllocationFinished(queue_id, allocation_id),
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn is_journal_enabled(&self) -> bool {
        self.inner.get().storage_sender.is_some()
    }

    fn send_event(
        &self,
        payload: EventPayload,
        now: Option<DateTime<Utc>>,
        forward_mode: ForwardMode,
    ) {
        let mut inner = self.inner.get_mut();
        if inner.storage_sender.is_none() && inner.client_listeners.is_empty() {
            return;
        }
        let event = Event {
            time: now.unwrap_or_else(Utc::now),
            payload,
        };
        inner.client_listeners.retain(|listener| {
            if listener.filter.check(&event.payload) {
                listener.sender.send(event.clone()).is_ok()
            } else {
                true
            }
        });
        if let Some(ref streamer) = inner.storage_sender
            && matches!(forward_mode, ForwardMode::StreamAndPersist)
            && streamer.send(EventStreamMessage::Event(event)).is_err()
        {
            log::error!("Event streaming queue has been closed.");
        }
    }

    pub fn on_server_stop(&self) {
        self.send_event(
            EventPayload::ServerStop,
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_server_start(&self, server_uid: &str) {
        self.send_event(
            EventPayload::ServerStart {
                server_uid: server_uid.to_string(),
            },
            None,
            ForwardMode::StreamAndPersist,
        );
    }

    pub fn on_task_notify(&self, task_id: TaskId, worker_id: WorkerId, message: Box<[u8]>) {
        self.send_event(
            EventPayload::TaskNotify(TaskNotification {
                task_id,
                worker_id,
                message,
            }),
            None,
            ForwardMode::Stream,
        );
    }

    pub fn on_job_idle(&self, job_id: JobId, now: DateTime<Utc>) {
        self.send_event(
            EventPayload::JobIdle(job_id),
            Some(now),
            ForwardMode::Stream,
        );
    }

    pub fn start_journal_replay(&self, history_sender: mpsc::UnboundedSender<Event>) {
        let inner = self.inner.get();
        if let Some(ref streamer) = inner.storage_sender
            && streamer
                .send(EventStreamMessage::ReplayJournal(history_sender))
                .is_err()
        {
            log::error!("Event streaming queue has been closed.");
        }
    }

    pub fn register_listener(
        &self,
        filter: EventFilter,
        sender: mpsc::UnboundedSender<Event>,
    ) -> u32 {
        let mut inner = self.inner.get_mut();
        let listener_id = inner
            .client_listeners
            .iter()
            .map(|x| x.id)
            .max()
            .unwrap_or(0)
            + 1;
        inner.client_listeners.push(EventListener {
            filter,
            sender,
            id: listener_id,
        });
        listener_id
    }

    pub fn unregister_listener(&self, listener_id: u32) {
        let mut inner = self.inner.get_mut();
        let p = inner
            .client_listeners
            .iter()
            .position(|listener| listener.id == listener_id)
            .unwrap();
        inner.client_listeners.remove(p);
    }

    pub fn start_flush(&self) -> Option<oneshot::Receiver<()>> {
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

    pub async fn flush_journal(&self) {
        if let Some(handle) = self.start_flush() {
            let _ = handle.await;
        };
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
