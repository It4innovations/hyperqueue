use crossterm::event;
use crossterm::event::Event::Key;
use futures::StreamExt;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::time::SystemTime;
use tokio::time::Duration;

use crate::client::globalsettings::GlobalSettings;
use crate::dashboard::data::DashboardData;
use crate::dashboard::data::{create_data_fetch_process, TimeMode, TimeRange};
use crate::dashboard::ui::screens::root_screen::RootScreen;
use crate::dashboard::ui::terminal::initialize_terminal;
use crate::dashboard::DEFAULT_LIVE_DURATION;
use crate::server::bootstrap::get_client_session;
use crate::server::event::Event;

/// Starts the dashboard UI with a keyboard listener and tick provider
pub async fn start_ui_loop(
    gsettings: &GlobalSettings,
    events: Option<Vec<Event>>,
) -> anyhow::Result<()> {
    let stream = events.is_none();
    let time_mode = match &events {
        Some(events) => {
            let end = match events.last() {
                Some(event) => event.time.into(),
                None => SystemTime::now(),
            };
            TimeMode::Fixed(TimeRange::new(end - Duration::from_secs(60 * 5), end))
        }
        None => TimeMode::Live(DEFAULT_LIVE_DURATION),
    };

    let mut dashboard_data = DashboardData::new(time_mode, stream);
    if let Some(events) = events {
        dashboard_data.push_new_events(events);
    }

    let mut root_screen = RootScreen::default();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);

    let mut data_fetch_process: Pin<Box<dyn Future<Output = anyhow::Result<()>>>> = if stream {
        let connection = get_client_session(gsettings.server_directory()).await?;
        Box::pin(create_data_fetch_process(connection, tx))
    } else {
        Box::pin(std::future::pending())
    };

    let mut terminal = initialize_terminal()?;
    let mut reader = event::EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));

    let res = loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(events) = msg else {
                    break Ok(());
                };
                dashboard_data.push_new_events(events);
            }
            res = &mut data_fetch_process => {
                break res;
            }
            event = reader.next() => {
                let Some(event) = event else {
                    break Ok(());
                };
                let event = match event {
                    Ok(event) => event,
                    Err(error) => break Err(error.into()),
                };
                if let Key(key) = event {
                    if let ControlFlow::Break(res) = root_screen.handle_key(key, &mut dashboard_data) {
                        break res;
                    }
                }
            }
            _ = tick.tick() => {
                root_screen.draw(&mut terminal, &dashboard_data);
            }
        }
    };
    ratatui::restore();
    res
}
