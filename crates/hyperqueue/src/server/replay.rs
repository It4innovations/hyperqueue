use crate::server::event::events::MonitoringEventPayload;
use crate::server::event::log::EventLogReader;
use crate::server::event::MonitoringEvent;
use std::path::Path;
use tako::messages::gateway::LostWorkerReason;

use crate::server::state::State;
use crate::server::worker::Worker;

pub async fn replay_server_state(state: &mut State, path: &Path) -> anyhow::Result<()> {
    let events = EventLogReader::open(path)?;
    for event in events {
        let event = event?;
        replay_event(state, event);
    }

    let connected_workers: Vec<_> = state
        .get_workers()
        .values()
        .filter(|w| w.ended().is_none())
        .map(|w| w.worker_id())
        .collect();
    for worker in connected_workers {
        state.remove_worker(worker, LostWorkerReason::ConnectionLost);
    }

    Ok(())
}

fn replay_event(state: &mut State, event: MonitoringEvent) {
    match event.payload {
        MonitoringEventPayload::WorkerConnected(id, configuration) => {
            state.add_worker(Worker::new(id, *configuration));
        }
        MonitoringEventPayload::WorkerLost(id, reason) => state.remove_worker(id, reason),
        MonitoringEventPayload::OverviewUpdate(_) => {}
    }
}
