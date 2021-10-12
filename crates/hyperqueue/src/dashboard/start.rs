use std::{io, thread};
use termion::event::Key;
use termion::input::TermRead;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Duration;

use crate::client::globalsettings::GlobalSettings;
use crate::dashboard::events::DashboardEvent;
use crate::dashboard::state::{DashboardScreen, DashboardState};
use crate::dashboard::ui::painter::DashboardPainter;
use crate::dashboard::utils::get_hw_overview;
use crate::server::bootstrap::get_client_connection;

///Starts the dashboard ui with the keyboard listener and tick provider
pub async fn start_ui_loop(
    mut painter: DashboardPainter,
    state: DashboardState,
    gsettings: &GlobalSettings,
) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    start_key_event_listener(tx.clone());

    let ticker = async move {
        // Update dashboard ui signal
        let mut tick_duration = tokio::time::interval(Duration::from_millis(500));
        loop {
            let _tick_result = tx.send(DashboardEvent::Tick);
            tick_duration.tick().await;
        }
    };
    let event_loop = async move {
        loop {
            if let Some(dashboard_event) = rx.recv().await {
                match dashboard_event {
                    DashboardEvent::KeyPressEvent(input) => {
                        println!("key pressed");
                        if input == Key::Char('q') {
                            // Quits the dashboard
                            break Ok(());
                        }
                    }

                    DashboardEvent::Tick => {
                        //Draw the correct dashboard ui according to the current ui state
                        match state.ui_state {
                            DashboardScreen::WorkerHwMonitorScreen => {
                                let overview = get_hw_overview(&mut connection).await?;
                                let _result = painter.draw_dashboard_home(overview);
                            }
                        }
                    }

                    DashboardEvent::ChangeUIStateEvent(_new_state) => {
                        //todo: change what is being drawn on the dashboard by changing the ui state!
                    }
                }
            }
        }
    };
    tokio::select! {
        _ = ticker => { Ok(()) }
        result = event_loop => { result }
    }
}

///Handles key press events when the dashboard_ui is active
fn start_key_event_listener(tx: UnboundedSender<DashboardEvent>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let stdin = io::stdin();
        for key in stdin.keys().flatten() {
            if let Err(err) = tx.send(DashboardEvent::KeyPressEvent(key)) {
                eprintln!("{}", err);
                return;
            }
        }
    })
}
