use ratatui::{DefaultTerminal, Frame};

pub type DashboardTerminal = DefaultTerminal;
pub type DashboardFrame<'a> = Frame<'a>;

pub fn initialize_terminal() -> std::io::Result<DashboardTerminal> {
    let mut terminal = ratatui::init();
    terminal.clear()?;
    Ok(terminal)
}
