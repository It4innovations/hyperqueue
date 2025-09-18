use crossterm::event;
use crossterm::event::Event::Key;
use futures::StreamExt;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::time::SystemTime;
use tokio::time::Duration;

use crate::dashboard::data::DashboardData;
use crate::dashboard::data::{TimeMode, TimeRange, create_data_fetch_process};
use crate::dashboard::ui::screens::root_screen::RootScreen;
use crate::dashboard::ui::terminal::initialize_terminal;
use crate::dashboard::{DEFAULT_LIVE_DURATION, PreloadedEvents};

/// Starts the dashboard UI with a keyboard listener and tick provider
pub async fn start_ui_loop(preloaded: PreloadedEvents) -> anyhow::Result<()> {
    let time_mode = match &preloaded {
        PreloadedEvents::FromJournal(events) => {
            let end = match events.last() {
                Some(event) => event.time.into(),
                None => SystemTime::now(),
            };
            TimeMode::Fixed(TimeRange::new(end - Duration::from_secs(60 * 5), end))
        }
        PreloadedEvents::FromServer { .. } => TimeMode::Live(DEFAULT_LIVE_DURATION),
    };

    let stream = match &preloaded {
        PreloadedEvents::FromJournal(_) => false,
        PreloadedEvents::FromServer { .. } => true,
    };
    let mut dashboard_data = DashboardData::new(time_mode, stream);
    let (events, session, warning) = match preloaded {
        PreloadedEvents::FromJournal(events) => (events, None, None),
        PreloadedEvents::FromServer {
            events,
            connection,
            journal_used,
        } => {
            let warning = if journal_used {
                None
            } else {
                Some(
                    "The server does not use a journal, historical data was reconstructed from the current server state. Consider passing `--journal <path>` to `hq server start` when using the dashboard.",
                )
            };
            (events, Some(connection), warning)
        }
    };
    dashboard_data.set_warning(warning);

    // Check that the events are sorted.
    assert!(events.is_sorted_by_key(|e| e.time));
    dashboard_data.push_new_events(events);

    let mut root_screen = RootScreen::default();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);

    let mut data_fetch_process: Pin<Box<dyn Future<Output = anyhow::Result<()>>>> =
        if let Some(session) = session {
            Box::pin(create_data_fetch_process(session, tx))
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
                if let Key(key) = event
                    && let ControlFlow::Break(res) = root_screen.handle_key(key, &mut dashboard_data) {
                        break res;
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
