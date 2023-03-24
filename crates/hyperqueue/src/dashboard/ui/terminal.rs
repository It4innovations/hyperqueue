use std::io::{stdout, Stdout};

use termion::input::MouseTerminal;
use termion::raw::{IntoRawMode, RawTerminal};
use termion::screen::AlternateScreen;
use tui::backend::TermionBackend as TerminalBackend;
use tui::{Frame, Terminal};

pub type Backend = TerminalBackend<AlternateScreen<MouseTerminal<RawTerminal<Stdout>>>>;
pub type DashboardTerminal = Terminal<Backend>;
pub type DashboardFrame<'a> = Frame<'a, Backend>;

pub fn initialize_terminal() -> std::io::Result<DashboardTerminal> {
    let stdout = AlternateScreen::from(MouseTerminal::from(stdout().into_raw_mode()?));
    let backend = TerminalBackend::new(stdout);
    Terminal::new(backend)
}
