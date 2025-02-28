use ratatui::layout::Rect;
use ratatui::style::Color;

use crate::dashboard::data::{DashboardData, TimeRange};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::chart::{
    RangeSteps, create_chart, create_dataset, get_time_as_secs, y_axis_steps,
};

#[derive(Default)]
pub struct WorkerCountChart {
    /// Worker count records that should be currently displayed.
    worker_counts: Vec<(f64, f64)>,
    range: TimeRange,
}

impl WorkerCountChart {
    pub fn update(&mut self, data: &DashboardData) {
        let range = data.current_time_range();
        let steps = RangeSteps::new(range, 25);
        self.worker_counts = steps
            .map(|time| {
                (
                    get_time_as_secs(time),
                    data.workers().query_connected_worker_ids_at(time).count() as f64,
                )
            })
            .collect();
        self.range = range;
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let max_workers_in_view = self
            .worker_counts
            .iter()
            .map(|record| record.1 as u64)
            .max()
            .unwrap_or(0)
            .max(4) as f64;

        let datasets = vec![create_dataset(
            &self.worker_counts,
            "Connected Workers",
            Color::White,
        )];

        let chart = create_chart(datasets, "Running Worker Count", self.range)
            .legend_position(None)
            .y_axis(y_axis_steps(0.0, max_workers_in_view, 4));
        frame.render_widget(chart, rect);
    }
}
