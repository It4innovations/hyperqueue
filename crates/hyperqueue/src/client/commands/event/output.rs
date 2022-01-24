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
        MonitoringEventPayload::WorkerConnected(id, ..) => json!({
            "type": "worker-connected",
            "id": id
        }),
        MonitoringEventPayload::WorkerLost(id, reason) => json!({
            "type": "worker-lost",
            "id": id,
            "reason": reason
        }),
        MonitoringEventPayload::OverviewUpdate(WorkerOverview { id, hw_state, .. }) => json!({
            "type": "worker-overview",
            "id": id,
            "hw_state": hw_state
        }),
    }
}
