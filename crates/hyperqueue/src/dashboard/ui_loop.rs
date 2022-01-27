use std::ops::ControlFlow;
use std::{io, thread};

use termion::event::Key;
use termion::input::TermRead;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Duration;

use crate::client::globalsettings::GlobalSettings;
use crate::dashboard::data;
use crate::dashboard::data::DashboardData;
use crate::dashboard::events::DashboardEvent;
use crate::dashboard::state::DashboardState;
use crate::dashboard::ui::terminal::{initialize_terminal, DashboardTerminal};
use crate::server::bootstrap::get_client_connection;

/// Starts the dashboard UI with a keyboard listener and tick provider
pub async fn start_ui_loop(gsettings: &GlobalSettings) -> anyhow::Result<()> {
    let connection = get_client_connection(gsettings.server_directory()).await?;

    // TODO: When we start the dashboard and connect to the server, the server may have already forgotten
    // some of its events. Therefore we should bootstrap the state with the most recent overview snapshot.
    let data = DashboardData::default();

    let (evt_tx, mut evt_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut state = DashboardState::new(data, evt_tx.clone());
    start_key_event_listener(evt_tx.clone());

    let ui_ticker = send_event_every(100, evt_tx, DashboardEvent::UiTick);

    let data_source = state.get_data_source().clone();
    let data_fetch_process =
        data::create_data_fetch_process(Duration::from_secs(1), data_source, connection);

    let mut terminal = initialize_terminal()?;

    let event_loop = async move {
        loop {
            if let Some(dashboard_event) = evt_rx.recv().await {
                match dashboard_event {
                    DashboardEvent::KeyPressEvent(input) => {
                        if let ControlFlow::Break(res) = handle_key(&mut state, input) {
                            break res;
                        }
                    }
                    DashboardEvent::UiTick => draw(&mut state, &mut terminal),
                    DashboardEvent::ScreenChange(new_state) => {
                        state.switch_screen(new_state);
                    }
                }
            }
        }
    };

    tokio::select! {
        _ = ui_ticker => {
            log::warn!("UI event process has ended");
            Ok(())
        }
        result = data_fetch_process => {
            log::warn!("Data fetch process has ended");
            result
        }
        result = event_loop => {
            log::warn!("Dashboard event loop has ended");
            result
        }
    }
}

fn handle_key(state: &mut DashboardState, input: Key) -> ControlFlow<anyhow::Result<()>> {
    if input == Key::Char('q') {
        // Quits the dashboard
        ControlFlow::Break(Ok(()))
    } else {
        let screen = state.get_current_screen_mut();
        screen.handle_key(input);
        ControlFlow::Continue(())
    }
}

fn draw(state: &mut DashboardState, terminal: &mut DashboardTerminal) {
    terminal
        .draw(|frame| {
            let data = state.get_data_source().clone();
            let screen = state.get_current_screen_mut();

            screen.update(&data.get());
            screen.draw(frame);
        })
        .expect("An error occurred while drawing the dashboard");
}

/// Handles key press events when the dashboard_ui is active
fn start_key_event_listener(tx: UnboundedSender<DashboardEvent>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let stdin = io::stdin();
        for key in stdin.keys().flatten() {
            if let Err(err) = tx.send(DashboardEvent::KeyPressEvent(key)) {
                eprintln!("Error in sending dashboard key: {}", err);
                return;
            }
        }
    })
}

/// Sends a dashboard event every n milliseconds
async fn send_event_every(
    n_milliseconds: u64,
    sender: UnboundedSender<DashboardEvent>,
    event_type: DashboardEvent,
) {
    let mut tick_duration = tokio::time::interval(Duration::from_millis(n_milliseconds));
    loop {
        if let Err(e) = sender.send(event_type) {
            log::error!("Error in producing dashboard events: {}", e);
            return;
        }
        tick_duration.tick().await;
    }
}
