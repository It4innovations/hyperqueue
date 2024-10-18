use std::time::SystemTime;

use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Chart, Dataset, GraphType};

use crate::dashboard::data::{DashboardData, TimeRange};
use crate::dashboard::ui::styles::chart_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::chart::{get_time_as_secs, x_axis_time_chart, y_axis_steps};

struct WorkerCountRecord {
    time: SystemTime,
    count: usize,
}

#[derive(Default)]
pub struct WorkerCountChart {
    /// Worker count records that should be currently displayed.
    worker_records: Vec<WorkerCountRecord>,
    range: TimeRange,
}

const TIME_STEPS: u32 = 25;

impl WorkerCountChart {
    pub fn update(&mut self, data: &DashboardData) {
        let range = data.current_time_range();
        let interval = range.end().duration_since(range.start()).unwrap() / TIME_STEPS;
        self.worker_records = (0..TIME_STEPS)
            .map(|step| range.start() + interval * step)
            .map(|time| WorkerCountRecord {
                time,
                count: data.workers().query_connected_worker_ids_at(time).count(),
            })
            .collect();
        self.range = range;
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let max_workers_in_view = self
            .worker_records
            .iter()
            .map(|record| record.count)
            .max()
            .unwrap_or(0)
            .max(4) as f64;

        let worker_counts: Vec<(f64, f64)> = self
            .worker_records
            .iter()
            .map(|record| (get_time_as_secs(record.time), record.count as f64))
            .collect();
        let datasets = vec![Dataset::default()
            .name("Connected workers")
            .marker(symbols::Marker::Dot)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::White))
            .data(&worker_counts)];

        let chart = Chart::new(datasets)
            .style(chart_style_deselected())
            .legend_position(None)
            .block(
                Block::default()
                    .title(Span::styled(
                        "Running worker count",
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ))
                    .borders(Borders::ALL),
            )
            .x_axis(x_axis_time_chart(self.range))
            .y_axis(y_axis_steps(0.0, max_workers_in_view, 4));
        frame.render_widget(chart, rect);
    }
}
