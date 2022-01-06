use std::ops::ControlFlow;
use std::{io, thread};

use termion::event::Key;
use termion::input::TermRead;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Duration;

use crate::client::commands::worker::get_worker_list;
use crate::client::globalsettings::GlobalSettings;
use crate::dashboard::events::DashboardEvent;
use crate::dashboard::state::DashboardState;
use crate::dashboard::ui::screen::ClusterState;
use crate::dashboard::ui::terminal::{initialize_terminal, DashboardTerminal};
use crate::dashboard::utils::get_hw_overview;
use crate::server::bootstrap::get_client_connection;
use crate::transfer::connection::ClientConnection;

/// Starts the dashboard UI with a keyboard listener and tick provider
pub async fn start_ui_loop(
    mut state: DashboardState,
    gsettings: &GlobalSettings,
) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    start_key_event_listener(tx.clone());

    let ui_ticker = send_event_every(100, tx.clone(), DashboardEvent::UiTick);
    let data_ticker = send_event_every(500, tx, DashboardEvent::DataTick);

    let mut terminal = initialize_terminal()?;

    let event_loop = async move {
        loop {
            if let Some(dashboard_event) = rx.recv().await {
                match dashboard_event {
                    DashboardEvent::KeyPressEvent(input) => {
                        if let ControlFlow::Break(res) = handle_key(&mut state, input) {
                            break res;
                        }
                    }
                    DashboardEvent::UiTick => draw(&mut state, &mut terminal),
                    // TODO: move to another thread in order to not block UI
                    DashboardEvent::DataTick => update(&mut state, &mut connection).await?,
                }
            }
        }
    };
    tokio::select! {
        _ = ui_ticker => { Ok(()) }
        _ = data_ticker => {Ok(()) }
        result = event_loop => { result }
    }
}

async fn update(
    state: &mut DashboardState,
    connection: &mut ClientConnection,
) -> anyhow::Result<()> {
    let overview = get_hw_overview().await?;
    let worker_info = get_worker_list(connection, false).await?;

    let screen = state.get_current_screen_mut();
    screen.update(ClusterState {
        overview,
        worker_info,
    });
    Ok(())
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
            let screen = state.get_current_screen_mut();
            screen.draw(frame);
        })
        .expect("An error occurred while drawing the dashboard");
}

///Handles key press events when the dashboard_ui is active
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

///Sends a dashboard event every n milliseconds
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
