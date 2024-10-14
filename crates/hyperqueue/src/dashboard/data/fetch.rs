use crate::dashboard::events::DashboardEvent;
use crate::rpc_call;
use crate::server::event::{Event, EventId};
use crate::transfer::connection::{ClientConnection, ClientSession};
use crate::transfer::messages::{FromClientMessage, ToClientMessage};
use std::time::Duration;
// use tako::gateway::MonitoringEventRequest;
use tokio::sync::mpsc::UnboundedSender;

pub async fn create_data_fetch_process(
    refresh_interval: Duration,
    mut session: ClientSession,
    sender: UnboundedSender<DashboardEvent>,
) -> anyhow::Result<()> {
    let mut tick_duration = tokio::time::interval(refresh_interval);

    let mut fetched_until: Option<EventId> = None;

    loop {
        let events = fetch_events_after(session.connection(), fetched_until).await?;
        // fetched_until = events
        //     .iter()
        //     .map(|event| event.id())
        //     .max()
        //     .or(fetched_until);

        sender.send(DashboardEvent::FetchedEvents(events))?;
        tick_duration.tick().await;
    }
}

/// Gets the events from the server after the event_id specified
async fn fetch_events_after(
    connection: &mut ClientConnection,
    after_id: Option<EventId>,
) -> crate::Result<Vec<Event>> {
    // rpc_call!(
    //     connection,
    //     FromClientMessage::MonitoringEvents (
    //         MonitoringEventRequest {
    //             after_id,
    //         }),
    //     ToClientMessage::MonitoringEventsResponse(response) => response
    // )
    // .await
    todo!()
}
