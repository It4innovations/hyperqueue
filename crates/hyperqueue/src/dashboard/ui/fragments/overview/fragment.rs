use termion::event::Key;

use crate::dashboard::ui::fragments::overview::cluster_overview_chart::ClusterOverviewChart;
use crate::dashboard::ui::fragments::overview::worker_utilization_table::WorkerUtilTable;
use crate::dashboard::ui::screen::{Fragment, FromFragmentMessage, ToFragmentMessage};
use crate::dashboard::ui::styles::{style_footer, style_header_text};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::controller::ScreenController;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct ClusterOverviewFragment {
    worker_util_table: WorkerUtilTable,
    cluster_overview: ClusterOverviewChart,
}

impl Fragment for ClusterOverviewFragment {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = OverviewFragmentLayout::new(&in_area);
        draw_text("HQ top", layout.header_chunk, frame, style_header_text());
        draw_text(
            "Press up_arrow and bottom_arrow to select a worker, press right_arrow for details about the selected worker",
            layout.footer_chunk,
            frame,
            style_footer(),
        );

        self.cluster_overview.draw(layout.worker_count_chunk, frame);
        self.worker_util_table
            .draw(layout.worker_util_table_chunk, frame);
    }

    fn update(&mut self, data: &DashboardData, _controller: &mut ScreenController) {
        self.worker_util_table.update(data);
        self.cluster_overview.update(data);
    }

    fn handle_message(&mut self, _message: ToFragmentMessage) {
        todo!()
    }

    /// Handles key presses for the components of the screen
    fn handle_key(
        &mut self,
        key: Key,
        _controller: &mut ScreenController,
    ) -> Option<FromFragmentMessage> {
        match key {
            Key::Down => {
                self.worker_util_table.select_next_worker();
                return self
                    .worker_util_table
                    .get_selected_item()
                    .map(FromFragmentMessage::WorkerIdChanged);
            }
            Key::Up => {
                self.worker_util_table.select_previous_worker();
                return self
                    .worker_util_table
                    .get_selected_item()
                    .map(FromFragmentMessage::WorkerIdChanged);
            }
            _ => {}
        }
        None
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
    header_chunk: Rect,
    worker_util_table_chunk: Rect,
    footer_chunk: Rect,
}

impl OverviewFragmentLayout {
    fn new(rect: &Rect) -> Self {
        let base_chunks = tui::layout::Layout::default()
            .constraints(vec![
                Constraint::Percentage(40),
                Constraint::Percentage(5),
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
            header_chunk: base_chunks[1],
            worker_util_table_chunk: base_chunks[2],
            footer_chunk: base_chunks[3],
        }
    }
}
