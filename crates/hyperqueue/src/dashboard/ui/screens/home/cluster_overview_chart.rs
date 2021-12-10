use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tui::layout::Rect;
use tui::style::{Color, Modifier, Style};
use tui::symbols;
use tui::text::Span;
use tui::widgets::{Axis, Block, Borders, Chart, Dataset};

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;

struct WorkerCountRecord {
    time: SystemTime,
    count: usize,
}

pub struct ClusterOverviewChart {
    /// Worker count records that should be currently displayed.
    worker_records: Vec<WorkerCountRecord>,
    /// The time for which the data is plotted for.
    view_size: Duration,
}

impl ClusterOverviewChart {
    pub fn update(&mut self, data: &DashboardData) {
        let end = SystemTime::now();
        let mut start = end - self.view_size;
        let mut times = vec![];

        while start <= end {
            times.push(start);
            start += Duration::from_secs(1);
        }

        self.worker_records = times
            .into_iter()
            .map(|time| WorkerCountRecord {
                time,
                count: data.worker_count_at(time),
            })
            .collect();
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let max_workers_in_view = self
            .worker_records
            .iter()
            .map(|record| record.count)
            .max()
            .unwrap_or(0) as f64;

        let worker_counts: Vec<(f64, f64)> = self
            .worker_records
            .iter()
            .map(|record| (get_time_as_secs(record.time) as f64, record.count as f64))
            .collect();
        let datasets = vec![Dataset::default()
            .name("workers_connected")
            .marker(symbols::Marker::Braille)
            .style(Style::default().fg(Color::White))
            .data(&worker_counts)];

        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title(Span::styled(
                        "Worker Connection History",
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ))
                    .borders(Borders::ALL),
            )
            .x_axis(
                Axis::default()
                    .title("time ->")
                    .style(Style::default().fg(Color::Gray))
                    .bounds([
                        get_time_as_secs(SystemTime::now() - self.view_size) as f64,
                        get_time_as_secs(SystemTime::now()) as f64,
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().fg(Color::Gray))
                    .bounds([0.0, max_workers_in_view])
                    .labels(vec![
                        Span::from(0.to_string()),
                        Span::from(max_workers_in_view.to_string()),
                    ]),
            );
        frame.render_widget(chart, rect);
    }
}

impl Default for ClusterOverviewChart {
    fn default() -> Self {
        Self {
            worker_records: vec![],
            view_size: Duration::from_secs(300),
        }
    }
}

fn get_time_as_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs()
}
