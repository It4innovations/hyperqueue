use termion::event::Key;

use crate::dashboard::ui::styles::{style_footer, style_header_text};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screens::overview_screen::overview::worker_count_chart::WorkerCountChart;
use crate::dashboard::ui::screens::overview_screen::overview::worker_utilization_table::WorkerUtilTable;
use tako::WorkerId;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct ClusterOverviewFragment {
    worker_util_table: WorkerUtilTable,
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
    pub fn handle_key(&mut self, key: Key) {
        match key {
            Key::Down => {
                self.worker_util_table.select_next_worker();
            }
            Key::Up => {
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
        let base_chunks = tui::layout::Layout::default()
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
