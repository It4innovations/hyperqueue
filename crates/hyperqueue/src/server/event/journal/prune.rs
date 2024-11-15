use crate::server::event::journal::{JournalReader, JournalWriter};
use crate::server::event::payload::EventPayload;
use crate::JobId;
use tako::{Set, WorkerId};

pub(crate) fn prune_journal(
    reader: &mut JournalReader,
    writer: &mut JournalWriter,
    live_job_ids: &Set<JobId>,
    live_worker_ids: &Set<WorkerId>,
) -> crate::Result<()> {
    for event in reader {
        let event = event?;
        let retain = match &event.payload {
            EventPayload::WorkerConnected(worker_id, _)
            | EventPayload::WorkerLost(worker_id, _) => live_worker_ids.contains(worker_id),
            EventPayload::WorkerOverviewReceived(overview) => {
                live_worker_ids.contains(&overview.id)
            }
            EventPayload::Submit { job_id, .. }
            | EventPayload::JobCompleted(job_id)
            | EventPayload::JobOpen(job_id, _)
            | EventPayload::JobClose(job_id)
            | EventPayload::TaskStarted { job_id, .. }
            | EventPayload::TaskFinished { job_id, .. }
            | EventPayload::TaskFailed { job_id, .. }
            | EventPayload::TaskCanceled { job_id, .. } => live_job_ids.contains(job_id),
            EventPayload::AllocationQueueCreated(_, _)
            | EventPayload::AllocationQueueRemoved(_)
            | EventPayload::AllocationQueued { .. }
            | EventPayload::AllocationStarted(_, _)
            | EventPayload::AllocationFinished(_, _)
            | EventPayload::ServerStart { .. }
            | EventPayload::ServerStop => true,
        };
        if retain {
            writer.store(event)?;
        }
    }
    Ok(())
}