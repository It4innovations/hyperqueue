use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Direction, Layout, Rect};

use tako::WorkerId;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screens::overview_screen::overview::worker_count_chart::WorkerCountChart;
use crate::dashboard::ui::screens::overview_screen::overview::worker_table::WorkerTable;
use crate::dashboard::ui::styles::style_footer;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

#[derive(Default)]
pub struct ClusterOverviewFragment {
    worker_util_table: WorkerTable,
    cluster_overview: WorkerCountChart,
}

impl ClusterOverviewFragment {
    pub fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = OverviewFragmentLayout::new(&in_area);
        draw_text(
            "<\u{21F5}> select worker, <i> worker details",
            layout.footer_chunk,
            frame,
            style_footer(),
        );

        self.cluster_overview.draw(layout.worker_count_chunk, frame);
        self.worker_util_table
            .draw(layout.worker_util_table_chunk, frame);
    }

    pub fn update(&mut self, data: &DashboardData) {
        self.worker_util_table.update(data);
        self.cluster_overview.update(data);
    }

    /// Handles key presses for the components of the screen
    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => {
                self.worker_util_table.select_next_worker();
            }
            KeyCode::Up => {
                self.worker_util_table.select_previous_worker();
            }
            _ => {}
        }
    }

    pub fn get_selected_worker(&self) -> Option<WorkerId> {
        self.worker_util_table.get_selected_item()
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
struct OverviewFragmentLayout {
    worker_count_chunk: Rect,
    _task_timeline_chart: Rect,
    worker_util_table_chunk: Rect,
    footer_chunk: Rect,
}

impl OverviewFragmentLayout {
    fn new(rect: &Rect) -> Self {
        let base_chunks = ratatui::layout::Layout::default()
            .constraints(vec![
                Constraint::Percentage(45),
                Constraint::Percentage(50),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        let info_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(base_chunks[0]);

        Self {
            worker_count_chunk: info_chunks[0],
            _task_timeline_chart: info_chunks[1],
            worker_util_table_chunk: base_chunks[1],
            footer_chunk: base_chunks[2],
        }
    }
}
