use std::time::SystemTime;
use termion::event::Key;

use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::styles::{style_footer, style_header_text};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::controller::ScreenController;
use crate::dashboard::ui::screens::worker::worker_config_table::WorkerConfigTable;
use tako::WorkerId;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct WorkerOverviewScreen {
    /// The worker info screen shows data for this worker
    worker_id: Option<WorkerId>,
    worker_info_table: WorkerConfigTable,
}

impl WorkerOverviewScreen {
    pub fn set_worker_id(&mut self, worker_id: WorkerId) {
        self.worker_id = Some(worker_id);
    }
}

impl Screen for WorkerOverviewScreen {
    fn draw(&mut self, frame: &mut DashboardFrame) {
        let layout = WorkerScreenLayout::new(frame);
        draw_text(
            format!(
                "Details for Worker {}",
                self.worker_id.unwrap_or_default().as_num()
            )
            .as_str(),
            layout.header_chunk,
            frame,
            style_header_text(),
        );
        draw_text(
            "Press left_arrow to go back to Cluster Overview",
            layout.footer_chunk,
            frame,
            style_footer(),
        );

        self.worker_info_table
            .draw(layout.worker_info_table_chunk, frame);
    }

    fn update(&mut self, data: &DashboardData, controller: &mut ScreenController) {
        let configuration = self
            .worker_id
            .and_then(|worker_id| {
                data.query_connected_worker_ids(SystemTime::now())
                    .find(|id| id == &worker_id)
            })
            .and_then(|worker_id| data.query_worker_info_for(&worker_id));
        match configuration {
            Some(configuration) => self.worker_info_table.update(configuration),
            None => controller.show_cluster_overview(),
        }
    }

    /// Handles key presses for the components of the screen
    fn handle_key(&mut self, key: Key, controller: &mut ScreenController) {
        if key == Key::Left {
            controller.show_cluster_overview();
        }
    }
}

/**
*  __________________________
   |     UChart | TChart   |
   |--------Header---------|
   |-----------------------|
   |     Info Table        |
   |-----------------------|
   |--------Footer---------|
   |-----------------------|
 **/
struct WorkerScreenLayout {
    _worker_util_chart_chunk: Rect,
    _worker_tasks_chart_chunk: Rect,
    header_chunk: Rect,
    worker_info_table_chunk: Rect,
    footer_chunk: Rect,
}

impl WorkerScreenLayout {
    fn new(frame: &DashboardFrame) -> Self {
        let base_chunks = tui::layout::Layout::default()
            .constraints(vec![
                Constraint::Percentage(45),
                Constraint::Percentage(5),
                Constraint::Percentage(45),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(frame.size());

        let info_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(30), Constraint::Percentage(70)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(base_chunks[0]);

        Self {
            _worker_util_chart_chunk: info_chunks[0],
            _worker_tasks_chart_chunk: info_chunks[1],
            header_chunk: base_chunks[1],
            worker_info_table_chunk: base_chunks[2],
            footer_chunk: base_chunks[3],
        }
    }
}

pub fn vertical_chunks(constraints: Vec<Constraint>, size: Rect) -> Vec<Rect> {
    tui::layout::Layout::default()
        .constraints(constraints.as_ref())
        .direction(Direction::Vertical)
        .split(size)
}

pub fn horizontal_chunks_with_margin(
    constraints: Vec<Constraint>,
    size: Rect,
    margin: u16,
) -> Vec<Rect> {
    Layout::default()
        .constraints(constraints.as_ref())
        .direction(Direction::Horizontal)
        .margin(margin)
        .split(size)
}
