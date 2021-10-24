use tui::layout::{Constraint, Rect};

use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::draw_utils::vertical_chunks;
use crate::dashboard::ui::screens::home::worker_utilization_table::HwUtilTable;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::header::draw_header;

pub struct HomeScreen {
    worker_table: HwUtilTable,
}

impl HomeScreen {
    pub fn new() -> Self {
        let worker_table = HwUtilTable::new();
        Self { worker_table }
    }
}

impl Screen for HomeScreen {
    fn draw(&self, frame: &mut DashboardFrame) {
        let layout = Layout::new(frame);
        draw_header(layout.header_chunk, frame, "HQ top");
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
