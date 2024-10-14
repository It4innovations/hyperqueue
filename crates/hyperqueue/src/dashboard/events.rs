use crate::server::event::Event;
use crossterm::event::KeyEvent;

#[derive(Debug, Clone)]
pub enum DashboardEvent {
    /// The event when a key is pressed
    KeyPressEvent(KeyEvent),
    /// Updates the dashboard ui with the latest data
    UiTick,
    /// New events were fetched from the server
    FetchedEvents(Vec<Event>),
}
