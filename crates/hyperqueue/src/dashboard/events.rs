use termion::event::Key;

#[derive(Debug, Clone, Copy)]
pub enum DashboardEvent {
    /// The event when a key is pressed
    KeyPressEvent(Key),
    /// Updates the dashboard ui with the latest data
    UiTick,
}
