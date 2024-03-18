use crate::client::output::json::format_datetime;
use crate::server::event::payload::EventPayload;
use crate::server::event::Event;
use crate::transfer::messages::JobDescription;
use serde_json::json;
use tako::worker::WorkerOverview;

pub fn format_event(event: Event) -> serde_json::Value {
    json!({
        "time": format_datetime(event.time),
        "event": format_payload(event.payload)
    })
}

fn format_payload(event: EventPayload) -> serde_json::Value {
    match event {
        EventPayload::WorkerConnected(id, configuration) => json!({
            "type": "worker-connected",
            "id": id,
            "extra": configuration.extra
        }),
        EventPayload::WorkerLost(id, reason) => json!({
            "type": "worker-lost",
            "id": id,
            "reason": reason
        }),
        EventPayload::WorkerOverviewReceived(WorkerOverview { id, hw_state, .. }) => {
            json!({
                "type": "worker-overview",
                "id": id,
                "hw-state": hw_state
            })
        }
        EventPayload::AllocationQueueCreated(id, _params) => {
            json!({
                "type": "autoalloc-queue-created",
                "queue-id": id
            })
        }
        EventPayload::AllocationQueueRemoved(id) => {
            json!({
                "type": "autoalloc-queue-removed",
                "queue-id": id
            })
        }
        EventPayload::AllocationQueued {
            queue_id,
            allocation_id,
            worker_count,
        } => {
            json!({
                "type": "autoalloc-allocation-queued",
                "queue-id": queue_id,
                "allocation-id": allocation_id,
                "worker-count": worker_count
            })
        }
        EventPayload::AllocationStarted(queue_id, allocation_id) => {
            json!({
                "type": "autoalloc-allocation-started",
                "queue-id": queue_id,
                "allocation-id": allocation_id,
            })
        }
        EventPayload::AllocationFinished(queue_id, allocation_id) => {
            json!({
                "type": "autoalloc-allocation-finished",
                "queue-id": queue_id,
                "allocation-id": allocation_id,
            })
        }
        EventPayload::TaskStarted {
            job_id, task_id, ..
        } => json!({
            "type": "task-started",
            "job": job_id,
            "task": task_id,
            "worker": -1
        }),
        EventPayload::TaskFinished { job_id, task_id } => json!({
            "type": "task-finished",
            "job": job_id,
            "task": task_id
        }),
        EventPayload::TaskCanceled { job_id, task_id } => json!({
            "type": "task-canceled",
            "job": job_id,
            "task": task_id
        }),
        EventPayload::TaskFailed {
            job_id,
            task_id,
            error,
        } => json!({
            "type": "task-failed",
            "job": job_id,
            "task": task_id,
            "error": error
        }),
        EventPayload::JobCreatedFull(job_id, job_info) => {
            todo!()
        }
        /*EventPayload::JobCreatedShort(job_id, job_desc) => json!({
            "type": "job-created",
            "job": job_id,
            "desc": JobInfoFormatter(&job_desc).to_json(),
        }),*/
        EventPayload::JobCompleted(job_id) => json!({
            "type": "job-completed",
            "job": job_id,
        }),
        EventPayload::ServerStop => {
            json!({
                "type": "server-stop",
            })
        }
    }
}

// We need a special formatter, since BString cannot be used as a hashmap key for JSON
struct JobInfoFormatter<'a>(&'a JobDescription);

impl<'a> JobInfoFormatter<'a> {
    fn to_json(&self) -> serde_json::Value {
        // Only format the job name for now
        json!({
            "name": self.0.name
        })
    }
}
