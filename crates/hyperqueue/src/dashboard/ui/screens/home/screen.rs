use termion::event::Key;
use tui::layout::{Constraint, Direction, Rect};

use crate::dashboard::ui::screen::{ClusterState, Screen};
use crate::dashboard::ui::screens::home::info_section::WorkerInfoTable;
use crate::dashboard::ui::screens::home::worker_utilization_table::WorkerUtilTable;
use crate::dashboard::ui::styles::style_header_text;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

#[derive(Default)]
pub struct HomeScreen {
    worker_info_table: WorkerInfoTable,
    worker_util_table: WorkerUtilTable,
}

impl Screen for HomeScreen {
    fn draw(&mut self, frame: &mut DashboardFrame) {
        let layout = Layout::new(frame);
        draw_text("HQ top", layout.header_chunk, frame, style_header_text());
        // Update the current selected worker for info table
        let selected_worker = self.worker_util_table.get_selected_item();
        self.worker_info_table.change_info(selected_worker);

        self.worker_info_table.draw(layout.info_chunk, frame);
        self.worker_util_table.draw(layout.body_chunk, frame);
    }

    fn update(&mut self, state: ClusterState) {
        self.worker_util_table.update(state.overview);
        self.worker_info_table.update(state.worker_info);
    }

    fn handle_key(&mut self, key: Key) {
        match key {
            Key::Down => self.worker_util_table.select_next_worker(),
            Key::Up => self.worker_util_table.select_previous_worker(),
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
    info_chunk: Rect,
    header_chunk: Rect,
    body_chunk: Rect,
}

impl Layout {
    fn new(frame: &DashboardFrame) -> Self {
        let base_chunks = vertical_chunks(
            vec![
                Constraint::Percentage(30),
                Constraint::Percentage(10),
                Constraint::Percentage(60),
            ],
            frame.size(),
        );
        Self {
            info_chunk: base_chunks[0],
            header_chunk: base_chunks[1],
            body_chunk: base_chunks[2],
        }
    }
}

pub fn vertical_chunks(constraints: Vec<Constraint>, size: Rect) -> Vec<Rect> {
    tui::layout::Layout::default()
        .constraints(constraints.as_ref())
        .direction(Direction::Vertical)
        .split(size)
}
