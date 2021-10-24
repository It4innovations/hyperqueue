use std::io::{stdout, Stdout};

use termion::input::MouseTerminal;
use termion::raw::{IntoRawMode, RawTerminal};
use termion::screen::AlternateScreen;
use tui::backend::TermionBackend;
use tui::{Frame, Terminal};

pub type Backend = TermionBackend<AlternateScreen<MouseTerminal<RawTerminal<Stdout>>>>;
pub type DashboardTerminal = Terminal<Backend>;
pub type DashboardFrame<'a> = Frame<'a, Backend>;

pub fn initialize_terminal() -> std::io::Result<DashboardTerminal> {
    let stdout = AlternateScreen::from(MouseTerminal::from(stdout().into_raw_mode()?));
    let backend = TermionBackend::new(stdout);
    Terminal::new(backend)
}
