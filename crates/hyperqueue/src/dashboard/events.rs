use crate::dashboard::state::DashboardScreen;
use termion::event::Key;

pub enum DashboardEvent {
    /// The event when a key is pressed
    KeyPressEvent(Key),
    /// Changes what is being drawn in the terminal
    ChangeUIStateEvent(DashboardScreen),
    /// Updates the dashboard with the latest data
    Tick,
}
