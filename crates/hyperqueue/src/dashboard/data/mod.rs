use crate::dashboard::data::worker_count::WorkerCountTimeline;
use std::time::{Duration, SystemTime};
use tako::common::WrappedRcRefCell;
use tako::messages::gateway::MonitoringEventRequest;
use tako::server::monitoring::MonitoringEvent;

use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};

pub mod worker_count;

#[derive(Default)]
pub struct DashboardData {
    /// The event_id until which the data has already been fetched for
    fetched_until: Option<u32>,
    /// All events received from the client
    events: Vec<MonitoringEvent>,
    worker_count_timeline: WorkerCountTimeline,
}

impl DashboardData {
    pub fn last_fetched_id(&self) -> Option<u32> {
        self.fetched_until
    }

    pub fn update_data(&mut self, mut events: Vec<MonitoringEvent>) {
        events.sort_unstable_by_key(|e| e.time());

        // Update maximum event ID
        self.fetched_until = events
            .iter()
            .map(|event| event.id())
            .max()
            .or(self.fetched_until);

        // Update data views
        self.worker_count_timeline.handle_new_events(&events);

        self.events.append(&mut events);
    }

    /// Calculates the number of workers connected to the cluster at the specified `time`.
    pub fn worker_count_at(&self, time: SystemTime) -> usize {
        self.worker_count_timeline.get_worker_count(time)
    }
}

pub async fn create_data_fetch_process(
    refresh_interval: Duration,
    data: WrappedRcRefCell<DashboardData>,
    mut connection: ClientConnection,
) -> anyhow::Result<()> {
    let mut tick_duration = tokio::time::interval(refresh_interval);
    loop {
        let fetched_until = data.get().last_fetched_id();
        let events = fetch_events_after(&mut connection, fetched_until).await?;
        data.get_mut().update_data(events);
        tick_duration.tick().await;
    }
}

/// Gets the events from the server after the event_id specified
async fn fetch_events_after(
    connection: &mut ClientConnection,
    after_id: Option<u32>,
) -> crate::Result<Vec<MonitoringEvent>> {
    let response = rpc_call!(
        connection,
        FromClientMessage::MonitoringEvents (
            MonitoringEventRequest {
                after_id,
            }),
        ToClientMessage::MonitoringEventsResponse(response) => response
    )
    .await;
    response
}
