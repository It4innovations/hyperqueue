use crate::server::event::MonitoringEvent;
use termion::event::Key;

#[derive(Debug, Clone)]
pub enum DashboardEvent {
    /// The event when a key is pressed
    KeyPressEvent(Key),
    /// Updates the dashboard ui with the latest data
    UiTick,
    /// New events were fetched from the server
    FetchedEvents(Vec<MonitoringEvent>),
}
