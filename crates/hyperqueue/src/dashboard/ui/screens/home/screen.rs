use termion::event::Key;

use crate::dashboard::ui::screen::{ClusterState, Screen};
use crate::dashboard::ui::screens::home::chart_section::ClusterOverviewChart;
use crate::dashboard::ui::screens::home::info_section::WorkerInfoTable;
use crate::dashboard::ui::screens::home::worker_utilization_table::WorkerUtilTable;
use crate::dashboard::ui::styles::{style_footer_text, style_header_text};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::ui::screens::home::tasks_table::WorkerJobsTable;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct HomeScreen {
    worker_info_table: WorkerInfoTable,
    worker_util_table: WorkerUtilTable,
    worker_job_table: WorkerJobsTable,
    table_state: HomeScreenTableState,

    worker_info_chart: ClusterOverviewChart,
}

pub enum HomeScreenTableState {
    WorkerUtilTable,
    WorkerJobsTable,
}

impl Screen for HomeScreen {
    fn draw(&mut self, frame: &mut DashboardFrame) {
        let layout = HomeLayout::new(frame);
        draw_text("HQ top", layout.header_chunk, frame, style_header_text());
        // Update the current selected worker for info table
        let selected_worker = self.worker_util_table.get_selected_item();
        self.worker_info_table.change_info(selected_worker);
        self.worker_job_table.update_current_worker(selected_worker);

        self.worker_info_chart.draw(layout.chart_chunk, frame);
        self.worker_info_table
            .draw(layout.info_chunk, frame, selected_worker);
        match &self.table_state {
            HomeScreenTableState::WorkerUtilTable => {
                self.worker_util_table.draw(layout.body_chunk, frame);
            }
            HomeScreenTableState::WorkerJobsTable => {
                self.worker_job_table
                    .draw(layout.body_chunk, frame, selected_worker);
            }
        }
        draw_text("Up/Down to select Worker, Right Arrow key to switch to Jobs for the Worker, Left to be back.", layout.footer_chunk, frame, style_footer_text());
    }

    fn update(&mut self, state: ClusterState) {
        self.worker_util_table.update(&state.overview);
        self.worker_info_table.update(state.worker_info);
        self.worker_info_chart.update(&state.overview);
        self.worker_job_table.update(state.worker_jobs_info);
    }

    fn handle_key(&mut self, key: Key) {
        match key {
            Key::Down => match self.table_state {
                HomeScreenTableState::WorkerUtilTable => {
                    self.worker_util_table.select_next_worker();
                }
                HomeScreenTableState::WorkerJobsTable => {
                    self.worker_job_table.select_next_job();
                }
            },
            Key::Up => match self.table_state {
                HomeScreenTableState::WorkerUtilTable => {
                    self.worker_util_table.select_previous_worker();
                }
                HomeScreenTableState::WorkerJobsTable => {
                    self.worker_job_table.select_previous_job();
                }
            },
            Key::Right => self.table_state = HomeScreenTableState::WorkerJobsTable,
            Key::Left => self.table_state = HomeScreenTableState::WorkerUtilTable,
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
    footer_chunk: Rect,
}

impl HomeLayout {
    fn new(frame: &DashboardFrame) -> Self {
        let base_chunks = vertical_chunks(
            vec![
                Constraint::Percentage(35),
                Constraint::Percentage(5),
                Constraint::Percentage(56),
                Constraint::Percentage(4),
            ],
            frame.size(),
        );
        let info_chunks = horizontal_chunks_with_margin(
            vec![Constraint::Percentage(30), Constraint::Percentage(70)],
            base_chunks[0],
            0,
        );
        Self {
            chart_chunk: info_chunks[0],
            info_chunk: info_chunks[1],
            header_chunk: base_chunks[1],
            body_chunk: base_chunks[2],
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

impl Default for HomeScreenTableState {
    fn default() -> Self {
        HomeScreenTableState::WorkerUtilTable
    }
}
