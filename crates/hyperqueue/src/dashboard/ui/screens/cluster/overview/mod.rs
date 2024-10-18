use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screens::cluster::overview::worker_count_chart::WorkerCountChart;
use crate::dashboard::ui::screens::cluster::overview::worker_table::WorkerTable;
use crate::dashboard::ui::styles::style_footer;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use tako::WorkerId;

mod worker_count_chart;
mod worker_table;

#[derive(Default)]
pub struct ClusterOverview {
    worker_count_chart: WorkerCountChart,
    worker_list_table: WorkerTable,
}

impl ClusterOverview {
    pub fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = OverviewLayout::new(&in_area);
        draw_text(
            "<\u{21F5}> select worker, <i> worker details",
            layout.footer_chunk,
            frame,
            style_footer(),
        );

        self.worker_count_chart
            .draw(layout.worker_count_chunk, frame);
        self.worker_list_table.draw(layout.worker_list_chunk, frame);
    }

    pub fn update(&mut self, data: &DashboardData) {
        self.worker_count_chart.update(data);
        self.worker_list_table.update(data);
    }

    /// Handles key presses for the components of the screen
    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => {
                self.worker_list_table.select_next_worker();
            }
            KeyCode::Up => {
                self.worker_list_table.select_previous_worker();
            }
            _ => {}
        }
    }

    pub fn get_selected_worker(&self) -> Option<WorkerId> {
        self.worker_list_table.get_selected_item()
    }
}

///   _________________________
///   |      Worker count     |
///   |-----------------------|
///   |      Worker list      |
///   |-----------------------|
///   |         Footer        |
///   -------------------------
struct OverviewLayout {
    worker_count_chunk: Rect,
    worker_list_chunk: Rect,
    footer_chunk: Rect,
}

impl OverviewLayout {
    fn new(rect: &Rect) -> Self {
        let base_chunks = Layout::default()
            .constraints(vec![
                Constraint::Percentage(45),
                Constraint::Percentage(50),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        Self {
            worker_count_chunk: base_chunks[0],
            worker_list_chunk: base_chunks[1],
            footer_chunk: base_chunks[2],
        }
    }
}
