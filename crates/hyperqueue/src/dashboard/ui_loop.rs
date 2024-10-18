use crossterm::event;
use crossterm::event::Event::Key;
use futures::StreamExt;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
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
    events: Vec<Event>,
    stream: bool,
) -> anyhow::Result<()> {
    let time_mode = if stream || events.is_empty() {
        TimeMode::Live(DEFAULT_LIVE_DURATION)
    } else {
        let end = events.last().unwrap().time.into();
        TimeMode::Fixed(TimeRange {
            start: end - Duration::from_secs(60 * 5),
            end,
        })
    };

    let mut dashboard_data = DashboardData::from_time_mode(time_mode);
    dashboard_data.push_new_events(events);

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
