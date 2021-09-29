use std::io::{stdout, Stdout};

use tako::messages::gateway::CollectedOverview;
use termion::input::MouseTerminal;
use termion::raw::{IntoRawMode, RawTerminal};
use termion::screen::AlternateScreen;
use tui::backend::TermionBackend;
use tui::layout::{Alignment, Constraint, Rect};
use tui::text::Spans;
use tui::widgets::{Block, Paragraph, Wrap};
use tui::{Frame, Terminal};

use crate::dashboard::ui::screens::draw_utils::vertical_chunks;
use crate::dashboard::ui::screens::hwutil_table::draw_worker_utilization_table;

/// Allows drawing to the terminal
pub type THandler = Terminal<FrameType>;
pub type FrameType = TermionBackend<AlternateScreen<MouseTerminal<RawTerminal<Stdout>>>>;

/**
 * Creates and Keeps a handle of the terminal,
 * Allows drawing to it
 */
pub struct DashboardPainter {
    terminal: THandler,
}

/**
*  _____________________
   |     HEADER        |
   |-------------------|
   |      BODY         |
   ---------------------
**/
#[derive(Clone)]
pub struct BaseUiChunks {
    pub tui_header_chunk: Rect,
    pub tui_body_chunk: Rect,
}

fn get_base_chunks(terminal_frame: &Frame<FrameType>) -> BaseUiChunks {
    let base_chunks = vertical_chunks(
        vec![Constraint::Percentage(20), Constraint::Percentage(80)],
        terminal_frame.size(),
    );
    BaseUiChunks {
        tui_header_chunk: base_chunks[0],
        tui_body_chunk: base_chunks[1],
    }
}

/// Returns handle to the terminal
fn initialize_terminal() -> futures::io::Result<THandler> {
    let stdout = AlternateScreen::from(MouseTerminal::from(stdout().into_raw_mode()?));
    let backend = TermionBackend::new(stdout);
    Terminal::new(backend)
}

impl DashboardPainter {
    ///Initialises the terminal
    pub fn init() -> Result<Self, anyhow::Error> {
        Ok(Self {
            terminal: initialize_terminal()?,
        })
    }

    /// Draws the dashboard home screen
    pub fn draw_dashboard_home(&mut self, overviews: CollectedOverview) -> anyhow::Result<()> {
        let _drawing_result = self.terminal.draw(|terminal_frame| {
            let base_chunks = get_base_chunks(terminal_frame);
            draw_dashboard_header(base_chunks.tui_header_chunk, terminal_frame);
            draw_worker_utilization_table(base_chunks.tui_body_chunk, terminal_frame, overviews);
        });
        Ok(())
    }
}

/// Draw the header of the dashboard
fn draw_dashboard_header(
    in_chunk: Rect,
    frame: &mut Frame<TermionBackend<AlternateScreen<MouseTerminal<RawTerminal<Stdout>>>>>,
) {
    let header_text = vec![Spans::from("Hyperqueue Dashboard")];
    let paragraph = Paragraph::new(header_text)
        .block(Block::default())
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, in_chunk);
}
