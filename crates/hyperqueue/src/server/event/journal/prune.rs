use crate::server::event::Event;
use crate::server::event::journal::{JournalReader, JournalWriter};
use crate::server::event::payload::EventPayload;
use chrono::Utc;
use tako::JobId;
use tako::{Set, WorkerId};

pub(crate) fn prune_journal(
    reader: &mut JournalReader,
    writer: &mut JournalWriter,
    live_job_ids: &Set<JobId>,
    live_worker_ids: &Set<WorkerId>,
    max_job_id: JobId,
    max_worker_id: WorkerId,
) -> crate::Result<()> {
    writer.store(Event {
        time: Utc::now(),
        payload: EventPayload::IdCounters {
            max_job_id,
            max_worker_id,
        },
    })?;
    for event in reader {
        let mut event = event?;
        let event = match &mut event.payload {
            EventPayload::WorkerConnected(worker_id, _)
            | EventPayload::WorkerLost(worker_id, _) => {
                live_worker_ids.contains(worker_id).then_some(event)
            }
            EventPayload::WorkerOverviewReceived(overview) => {
                live_worker_ids.contains(&overview.id).then_some(event)
            }
            EventPayload::Submit { job_id, .. }
            | EventPayload::JobCompleted(job_id)
            | EventPayload::JobOpen(job_id, _)
            | EventPayload::JobClose(job_id)
            | EventPayload::JobCancel { job_id, .. } => {
                live_job_ids.contains(job_id).then_some(event)
            }
            EventPayload::TaskStarted { task_id, .. }
            | EventPayload::TaskFinished { task_id, .. }
            | EventPayload::TaskFailed { task_id, .. } => {
                live_job_ids.contains(&task_id.job_id()).then_some(event)
            }
            EventPayload::TasksAborted { task_ids } | EventPayload::TasksCanceled { task_ids } => {
                task_ids.retain(|id| live_job_ids.contains(&id.job_id()));
                (!task_ids.is_empty()).then_some(event)
            }
            EventPayload::AllocationQueueCreated(_, _)
            | EventPayload::AllocationQueueRemoved(_)
            | EventPayload::AllocationQueued { .. }
            | EventPayload::AllocationStarted(_, _)
            | EventPayload::AllocationFinished(_, _)
            | EventPayload::ServerStart { .. }
            | EventPayload::TaskNotify(_)
            | EventPayload::JobIdle(_)
            | EventPayload::ServerStop => Some(event),
            // A fresh one was written at the beginning
            EventPayload::IdCounters { .. } => None,
        };
        if let Some(event) = event {
            writer.store(event)?;
        }
    }
    Ok(())
}
