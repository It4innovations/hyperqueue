use tako::messages::gateway::CollectedOverview;
use termion::event::Key;
use tui::layout::{Constraint, Rect};

use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::draw_utils::vertical_chunks;
use crate::dashboard::ui::screens::home::worker_utilization_table::WorkerUtilTable;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

#[derive(Default)]
pub struct HomeScreen {
    worker_table: WorkerUtilTable,
}

impl Screen for HomeScreen {
    fn draw(&mut self, frame: &mut DashboardFrame) {
        let layout = Layout::new(frame);
        draw_text(layout.header_chunk, frame, "HQ top");
        self.worker_table.draw(layout.body_chunk, frame);
    }

    fn update(&mut self, overview: CollectedOverview) {
        self.worker_table.update(overview);
    }

    fn handle_key(&mut self, key: Key) {
        match key {
            Key::Down => self.worker_table.select_next_worker(),
            Key::Up => self.worker_table.select_previous_worker(),
            _ => {}
        }
    }
}

/**
*  _____________________
   |     HEADER        |
   |-------------------|
   |      BODY         |
   ---------------------
 **/
struct Layout {
    header_chunk: Rect,
    body_chunk: Rect,
}

impl Layout {
    fn new(frame: &DashboardFrame) -> Self {
        let base_chunks = vertical_chunks(
            vec![Constraint::Percentage(10), Constraint::Percentage(90)],
            frame.size(),
        );
        Self {
            header_chunk: base_chunks[0],
            body_chunk: base_chunks[1],
        }
    }
}
