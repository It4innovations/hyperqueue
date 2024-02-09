use std::io::{stdout, Stdout};

use ratatui::backend::TermionBackend as TerminalBackend;
use ratatui::{Frame, Terminal};
use termion::input::MouseTerminal;
use termion::raw::{IntoRawMode, RawTerminal};
use termion::screen::AlternateScreen;

pub type Backend = TerminalBackend<AlternateScreen<MouseTerminal<RawTerminal<Stdout>>>>;
pub type DashboardTerminal = Terminal<Backend>;
pub type DashboardFrame<'a> = Frame<'a>;

pub fn initialize_terminal() -> std::io::Result<DashboardTerminal> {
    let stdout = AlternateScreen::from(MouseTerminal::from(stdout().into_raw_mode()?));
    let backend = TerminalBackend::new(stdout);
    Terminal::new(backend)
}
