mod data;
mod ui;
mod ui_loop;
mod utils;

pub use ui_loop::start_ui_loop;

use crate::client::globalsettings::GlobalSettings;
use crate::common::cli::DashboardCommand;
use crate::rpc_call;
use crate::server::bootstrap::get_client_session;
use crate::server::event::Event;
use crate::server::event::journal::JournalReader;
use crate::server::event::streamer::EventFilter;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    FromClientMessage, StreamEvents, StreamEventsMode, ToClientMessage,
};
use std::time::Duration;

// The time range in which the live timeline is display ([now() - duration, now()])
const DEFAULT_LIVE_DURATION: Duration = Duration::from_secs(60 * 10);

pub enum PreloadedEvents {
    FromJournal(Vec<Event>),
    FromServer {
        events: Vec<Event>,
        connection: ClientSession,
        // If true, the server stores events into a journal.
        journal_used: bool,
    },
}

/// Preload initial events for the dashboard.
/// Either loads them from a journal on disk, or streams past events from the server,
/// until a boundary is hit.
pub async fn preload_dashboard_events(
    gsettings: &GlobalSettings,
    cmd: DashboardCommand,
) -> anyhow::Result<PreloadedEvents> {
    match cmd {
        DashboardCommand::Stream => {
            let mut session = get_client_session(gsettings.server_directory()).await?;
            let connection = session.connection();
            let mut events = Vec::new();

            let server_info = rpc_call!(connection, FromClientMessage::ServerInfo, ToClientMessage::ServerInfo(info) => info).await?;

            // Start streaming events
            connection
                .send(FromClientMessage::StreamEvents(StreamEvents {
                    mode: StreamEventsMode::PastAndLiveEvents,
                    enable_worker_overviews: true,
                    filter: EventFilter::all_events(),
                }))
                .await?;

            println!("Streaming events from the server");
            while let Some(message) = connection.receive().await {
                let message = message?;
                match message {
                    ToClientMessage::Event(event) => {
                        events.push(event);
                    }
                    ToClientMessage::EventLiveBoundary => {
                        // All past events have been received, finish preloading
                        println!("Loaded {} events", events.len());
                        return Ok(PreloadedEvents::FromServer {
                            events,
                            connection: session,
                            journal_used: server_info.journal_path.is_some(),
                        });
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Dashboard received unexpected message {message:?}"
                        ));
                    }
                };
            }
            Err(anyhow::anyhow!("Server connection ended unexpectedly"))
        }
        DashboardCommand::Replay { journal } => {
            // In theory, we could also load the events from the server using `live_events: false`,
            // but loading it from the journal file directly allows us to run dashboard post-mortem,
            // without the server having to run.
            // It also means that the journal file has to be accessible from the client though,
            // but that should usually be the case.
            println!("Loading journal {}", journal.display());
            let mut journal = JournalReader::open(&journal)?;
            let events: Vec<Event> = journal.collect::<Result<_, _>>()?;
            println!("Loaded {} events", events.len());
            Ok(PreloadedEvents::FromJournal(events))
        }
    }
}
