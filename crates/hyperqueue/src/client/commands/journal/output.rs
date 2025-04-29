use crate::client::output::json::format_datetime;
use crate::server::event::Event;
use crate::server::event::payload::EventPayload;
use crate::transfer::messages::{JobDescription, SubmitRequest};
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
        EventPayload::WorkerOverviewReceived(overview) => {
            let WorkerOverview {
                id,
                hw_state,
                data_node,
                ..
            } = *overview;
            json!({
                "type": "worker-overview",
                "id": id,
                "hw-state": hw_state,
                "data-node": data_node,
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
        EventPayload::TaskStarted { task_id, .. } => json!({
            "type": "task-started",
            "job": task_id.job_id(),
            "task": task_id.job_task_id(),
            "worker": -1
        }),
        EventPayload::TaskFinished { task_id } => json!({
            "type": "task-finished",
            "job": task_id.job_id(),
            "task": task_id.job_task_id(),
        }),
        EventPayload::TaskCanceled { task_id } => json!({
            "type": "task-canceled",
            "job": task_id.job_id(),
            "task": task_id.job_task_id(),
        }),
        EventPayload::TaskFailed { task_id, error } => json!({
            "type": "task-failed",
            "job": task_id.job_id(),
            "task": task_id.job_task_id(),
            "error": error
        }),
        EventPayload::Submit {
            job_id,
            closed_job,
            serialized_desc,
        } => {
            let submit: SubmitRequest = serialized_desc.deserialize().expect("Invalid submit data");
            json!({
                "type": "job-created",
                "job": job_id,
                "closed_job": closed_job,
                "desc": JobInfoFormatter(&submit.job_desc).to_json(),
            })
        }
        EventPayload::JobCompleted(job_id) => json!({
            "type": "job-completed",
            "job": job_id,
        }),
        EventPayload::ServerStart { server_uid } => {
            json!({
                "type": "server-start",
                "server_uid": server_uid
            })
        }
        EventPayload::ServerStop => {
            json!({
                "type": "server-stop",
            })
        }
        EventPayload::JobOpen(job_id, job_desc) => {
            json!({
                "type": "job-open",
                "job_id": job_id,
                "name": job_desc.name,
                "max_fails": job_desc.max_fails,
            })
        }
        EventPayload::JobClose(job_id) => {
            json!({
                "type": "job-close",
                "job_id": job_id
            })
        }
    }
}

// We need a special formatter, since BString cannot be used as a hashmap key for JSON
struct JobInfoFormatter<'a>(&'a JobDescription);

impl JobInfoFormatter<'_> {
    fn to_json(&self) -> serde_json::Value {
        // Only format the job name for now
        json!({
            "name": self.0.name
        })
    }
}
