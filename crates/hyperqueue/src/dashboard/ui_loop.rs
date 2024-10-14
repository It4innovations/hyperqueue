use crossterm::event;
use crossterm::event::Event::Key;
use crossterm::event::KeyEventKind;
use std::io::Write;
use std::ops::ControlFlow;
use std::{io, thread};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Duration;

use crate::client::globalsettings::GlobalSettings;
use crate::dashboard::data::create_data_fetch_process;
use crate::dashboard::data::DashboardData;
use crate::dashboard::events::DashboardEvent;
use crate::dashboard::ui::screens::root_screen::RootScreen;
use crate::dashboard::ui::terminal::initialize_terminal;
use crate::server::bootstrap::get_client_session;

/// Starts the dashboard UI with a keyboard listener and tick provider
pub async fn start_ui_loop(gsettings: &GlobalSettings) -> anyhow::Result<()> {
    // let connection = get_client_session(gsettings.server_directory()).await?;

    // TODO: When we start the dashboard and connect to the server, the server may have already forgotten
    // some of its events. Therefore we should bootstrap the state with the most recent overview snapshot.
    let mut dashboard_data = DashboardData::default();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut root_screen = RootScreen::default();

    // let ui_ticker = send_event_repeatedly(
    //     Duration::from_millis(100),
    //     tx.clone(),
    //     DashboardEvent::UiTick,
    // );
    // let data_fetch_process = create_data_fetch_process(Duration::from_secs(1), connection, tx);

    let mut terminal = initialize_terminal()?;

    let res = loop {
        root_screen.draw(&mut terminal, &dashboard_data);
        if let Key(key) = event::read()? {
            if let ControlFlow::Break(res) = root_screen.handle_key(key, &mut dashboard_data) {
                break res;
            }
        }
    };
    ratatui::restore();
    return res;

    let event_loop = async {
        while let Some(dashboard_event) = rx.recv().await {
            match dashboard_event {
                DashboardEvent::KeyPressEvent(input) => {
                    if let ControlFlow::Break(res) =
                        root_screen.handle_key(input, &mut dashboard_data)
                    {
                        return res;
                    }
                }
                DashboardEvent::UiTick => {}
                DashboardEvent::FetchedEvents(events) => {
                    dashboard_data.push_new_events(events);
                }
            }
        }
        Ok(())
    };
}

/// Sends a dashboard event repeatedly, with the specified interval.
async fn send_event_repeatedly(
    interval: Duration,
    sender: UnboundedSender<DashboardEvent>,
    event_type: DashboardEvent,
) {
    let mut tick_duration = tokio::time::interval(interval);
    loop {
        if let Err(e) = sender.send(event_type.clone()) {
            log::error!("Error in producing dashboard events: {}", e);
            return;
        }
        tick_duration.tick().await;
    }
}
