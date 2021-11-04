use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tako::messages::gateway::CollectedOverview;

use crate::dashboard::ui::terminal::DashboardFrame;
use tui::layout::Rect;
use tui::style::{Color, Modifier, Style};
use tui::symbols;
use tui::text::Span;
use tui::widgets::{Axis, Block, Borders, Chart, Dataset};

/// Stores the number of connected workers at a time.
struct WorkerCountRecord {
    time: SystemTime,
    worker_count: u32,
}

pub struct ClusterOverviewChart {
    /// Worker count at different times.
    worker_records: Vec<WorkerCountRecord>,
    /// The time for which the data is plotted for.
    view_size: Duration,
}

impl ClusterOverviewChart {
    pub fn update(&mut self, overview: &CollectedOverview) {
        self.worker_records.push(WorkerCountRecord {
            time: SystemTime::now(),
            worker_count: overview.worker_overviews.len() as u32,
        });
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let workers_in_view = self.get_workers_in_view();
        let max_workers_in_view = workers_in_view
            .iter()
            .map(|&(_, count)| count)
            .max()
            .unwrap_or(0) as f64;

        let worker_counts: Vec<(f64, f64)> = workers_in_view
            .iter()
            .map(|x| (x.0 as f64, x.1 as f64))
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

    /// Returns the worker connection chart data for the view size.
    fn get_workers_in_view(&self) -> Vec<(u64, u64)> {
        let current_time = SystemTime::now();
        self.worker_records
            .iter()
            .filter(|count| count.time > current_time - self.view_size)
            .map(|record| (get_time_as_secs(record.time), record.worker_count as u64))
            .collect()
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
