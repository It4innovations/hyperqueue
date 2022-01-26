use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tako::WorkerId;
use tui::layout::Rect;
use tui::style::{Color, Modifier, Style};
use tui::symbols;
use tui::text::Span;
use tui::widgets::{Axis, Block, Borders, Chart, Dataset};

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;

struct WorkerCoreUtil {
    time: SystemTime,
    avg_util: f32,
}

pub struct WorkerAvgCpuUtilChart {
    /// Worker cpu util records that should be currently displayed.
    cpu_util_records: Vec<WorkerCoreUtil>,
    /// The time for which the data is plotted for.
    view_size: Duration,
}

impl WorkerAvgCpuUtilChart {
    pub fn update(&mut self, data: &DashboardData, worker_id: WorkerId) {
        let end = SystemTime::now();
        let mut start = end - self.view_size;
        let mut times = vec![];
        while start <= end {
            times.push(start);
            start += Duration::from_secs(1);
        }

        self.cpu_util_records = times
            .into_iter()
            .map(|time| {
                let wkr_cpu_util = data.query_worker_cpu_util(worker_id, time);
                let avg_util = wkr_cpu_util.iter().sum::<f32>() / wkr_cpu_util.len() as f32;

                WorkerCoreUtil { time, avg_util }
            })
            .collect();
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let max_util_in_view = self
            .cpu_util_records
            .iter()
            .map(|record| record.avg_util)
            .fold(f32::NAN, f32::max) as f64;

        let avg_cpu_util: Vec<(f64, f64)> = self
            .cpu_util_records
            .iter()
            .map(|record| (get_time_as_secs(record.time) as f64, record.avg_util as f64))
            .collect();
        let datasets = vec![Dataset::default()
            .name("cpu_%")
            .marker(symbols::Marker::Braille)
            .style(Style::default().fg(Color::White))
            .data(&avg_cpu_util)];

        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title(Span::styled(
                        "Average CPU Usage",
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
                    .bounds([0.0, 100.0])
                    .labels(vec![
                        Span::from(0.to_string()),
                        Span::from(max_util_in_view.to_string()),
                    ]),
            );
        frame.render_widget(chart, rect);
    }
}

impl Default for WorkerAvgCpuUtilChart {
    fn default() -> Self {
        Self {
            cpu_util_records: vec![],
            view_size: Duration::from_secs(300),
        }
    }
}

fn get_time_as_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs()
}
