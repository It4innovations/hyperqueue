use termion::event::Key;

use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::styles::style_header_text;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screens::worker::worker_info_table::WorkerInfoTable;
use crate::dashboard::ui::screens::worker::worker_tasks_chart::WorkerTasksChart;
use crate::dashboard::ui::screens::worker::worker_util_chart::WorkerAvgCpuUtilChart;
use tako::WorkerId;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct WorkerInfoScreen {
    /// The worker info screen shows data for this worker
    worker_id: WorkerId,

    worker_tasks_chart: WorkerTasksChart,
    worker_info_table: WorkerInfoTable,
    worker_util_chart: WorkerAvgCpuUtilChart,
}

impl Screen for WorkerInfoScreen {
    fn draw(&mut self, frame: &mut DashboardFrame) {
        let layout = WorkerScreenLayout::new(frame);
        draw_text(
            "Details for worker: {self.worker_id}",
            layout.header_chunk,
            frame,
            style_header_text(),
        );

        self.worker_util_chart
            .draw(layout.worker_util_chart_chunk, frame);
        self.worker_tasks_chart
            .draw(layout.worker_tasks_chart_chunk, frame);
        self.worker_info_table
            .draw(layout.worker_info_table_chunk, frame);
    }

    fn update(&mut self, data: &DashboardData) {
        self.worker_tasks_chart.update(data, self.worker_id);
        self.worker_util_chart.update(data, self.worker_id);
        self.worker_info_table.update(data, Some(self.worker_id));
        //fixme: temporary hack
    }

    /// Handles key presses for the components of the screen
    fn handle_key(&mut self, _key: Key) {
        // none for now
    }
}

/**
*  __________________________
   |     UChart | TChart   |
   |--------Header---------|
   |-----------------------|
   |     Info Table        |
   -------------------------
 **/
struct WorkerScreenLayout {
    worker_util_chart_chunk: Rect,
    worker_tasks_chart_chunk: Rect,
    header_chunk: Rect,
    worker_info_table_chunk: Rect,
}

impl WorkerScreenLayout {
    fn new(frame: &DashboardFrame) -> Self {
        let base_chunks = tui::layout::Layout::default()
            .constraints(vec![
                Constraint::Percentage(30),
                Constraint::Percentage(10),
                Constraint::Percentage(30),
            ])
            .direction(Direction::Vertical)
            .split(frame.size());

        let info_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(30), Constraint::Percentage(70)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(base_chunks[0]);

        Self {
            worker_util_chart_chunk: info_chunks[0],
            worker_tasks_chart_chunk: info_chunks[1],
            header_chunk: base_chunks[1],
            worker_info_table_chunk: base_chunks[2],
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
