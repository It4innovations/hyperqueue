use crate::client::output::json::format_datetime;
use crate::server::event::events::MonitoringEventPayload;
use crate::server::event::MonitoringEvent;
use serde_json::json;
use tako::messages::worker::WorkerOverview;

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
                "descriptor-id": id
            })
        }
        MonitoringEventPayload::AllocationQueueRemoved(id) => {
            json!({
                "type": "autoalloc-queue-removed",
                "descriptor-id": id
            })
        }
        MonitoringEventPayload::AllocationQueued {
            descriptor_id,
            allocation_id,
            worker_count,
        } => {
            json!({
                "type": "autoalloc-allocation-queued",
                "descriptor-id": descriptor_id,
                "allocation-id": allocation_id,
                "worker-count": worker_count
            })
        }
        MonitoringEventPayload::AllocationStarted(descriptor_id, allocation_id) => {
            json!({
                "type": "autoalloc-allocation-started",
                "descriptor-id": descriptor_id,
                "allocation-id": allocation_id,
            })
        }
        MonitoringEventPayload::AllocationFinished(descriptor_id, allocation_id) => {
            json!({
                "type": "autoalloc-allocation-finished",
                "descriptor-id": descriptor_id,
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
    }
}
