use crate::client::output::json::format_datetime;
use crate::server::event::events::{JobInfo, MonitoringEventPayload};
use crate::server::event::MonitoringEvent;
use serde_json::json;
use tako::worker::WorkerOverview;

pub fn format_event(event: MonitoringEvent) -> serde_json::Value {
    json!({
        "id": event.id,
        "time": format_datetime(event.time),
        "event": format_payload(event.payload)
    })
}

fn format_payload(event: MonitoringEventPayload) -> serde_json::Value {
    match event {
        MonitoringEventPayload::WorkerConnected(id, configuration) => json!({
            "type": "worker-connected",
            "id": id,
            "extra": configuration.extra
        }),
        MonitoringEventPayload::WorkerLost(id, reason) => json!({
            "type": "worker-lost",
            "id": id,
            "reason": reason
        }),
        MonitoringEventPayload::WorkerOverviewReceived(WorkerOverview { id, hw_state, .. }) => {
            json!({
                "type": "worker-overview",
                "id": id,
                "hw-state": hw_state
            })
        }
        MonitoringEventPayload::AllocationQueueCreated(id, _params) => {
            json!({
                "type": "autoalloc-queue-created",
                "queue-id": id
            })
        }
        MonitoringEventPayload::AllocationQueueRemoved(id) => {
            json!({
                "type": "autoalloc-queue-removed",
                "queue-id": id
            })
        }
        MonitoringEventPayload::AllocationQueued {
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
        MonitoringEventPayload::AllocationStarted(queue_id, allocation_id) => {
            json!({
                "type": "autoalloc-allocation-started",
                "queue-id": queue_id,
                "allocation-id": allocation_id,
            })
        }
        MonitoringEventPayload::AllocationFinished(queue_id, allocation_id) => {
            json!({
                "type": "autoalloc-allocation-finished",
                "queue-id": queue_id,
                "allocation-id": allocation_id,
            })
        }
        MonitoringEventPayload::TaskStarted { task_id, worker_id } => json!({
            "type": "task-started",
            "id": task_id,
            "worker": worker_id
        }),
        MonitoringEventPayload::TaskFinished(task_id) => json!({
            "type": "task-finished",
            "id": task_id
        }),
        MonitoringEventPayload::TaskFailed(task_id) => json!({
            "type": "task-failed",
            "id": task_id
        }),
        MonitoringEventPayload::JobCreated(job_id, job_info) => json!({
            "type": "job-created",
            "job-id": job_id,
            "job-info": JobInfoFormatter(&job_info).to_json(),
        }),
        MonitoringEventPayload::JobCompleted(job_id, completion_date) => json!({
            "type": "job-completed",
            "job-id": job_id,
            "completion-date": completion_date
        }),
    }
}

// We need a special formatter, since BString cannot be used as a hashmap key for JSON
struct JobInfoFormatter<'a>(&'a JobInfo);

impl<'a> JobInfoFormatter<'a> {
    fn to_json(&self) -> serde_json::Value {
        // Only format the job name for now
        json!({
            "name": self.0.name
        })
    }
}
