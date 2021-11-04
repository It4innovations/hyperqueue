use termion::event::Key;

use crate::dashboard::ui::screen::{ClusterState, Screen};
use crate::dashboard::ui::screens::home::chart_section::ClusterOverviewChart;
use crate::dashboard::ui::screens::home::info_section::WorkerInfoTable;
use crate::dashboard::ui::screens::home::worker_utilization_table::WorkerUtilTable;
use crate::dashboard::ui::styles::style_header_text;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct HomeScreen {
    worker_info_table: WorkerInfoTable,
    worker_util_table: WorkerUtilTable,
    worker_info_chart: ClusterOverviewChart,
}

impl Screen for HomeScreen {
    fn draw(&mut self, frame: &mut DashboardFrame) {
        let layout = HomeLayout::new(frame);
        draw_text("HQ top", layout.header_chunk, frame, style_header_text());
        // Update the current selected worker for info table
        let selected_worker = self.worker_util_table.get_selected_item();
        self.worker_info_table.change_info(selected_worker);

        self.worker_info_chart.draw(layout.chart_chunk, frame);
        self.worker_info_table.draw(layout.info_chunk, frame);
        self.worker_util_table.draw(layout.body_chunk, frame);
    }

    fn update(&mut self, state: ClusterState) {
        self.worker_util_table.update(&state.overview);
        self.worker_info_table.update(state.worker_info);
        self.worker_info_chart.update(&state.overview)
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
*  __________________________
   |     Chart |    Info   |
   |--------Header---------|
   |-----------------------|
   |          BODY         |
   -------------------------
 **/
struct HomeLayout {
    chart_chunk: Rect,
    info_chunk: Rect,
    header_chunk: Rect,
    body_chunk: Rect,
}

impl HomeLayout {
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
            chart_chunk: info_chunks[0],
            info_chunk: info_chunks[1],
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
