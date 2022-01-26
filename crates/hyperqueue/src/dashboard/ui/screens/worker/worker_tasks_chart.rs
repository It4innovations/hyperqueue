use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tako::WorkerId;
use tui::layout::Rect;
use tui::style::{Color, Modifier, Style};
use tui::symbols;
use tui::text::Span;
use tui::widgets::{Axis, Block, Borders, Chart, Dataset};

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;

struct TaskCountRecord {
    time: SystemTime,
    count: usize,
}

pub struct WorkerTasksChart {
    /// Task count records that should be currently displayed.
    task_records: Vec<TaskCountRecord>,
    /// The time for which the data is plotted for.
    view_size: Duration,
}

impl WorkerTasksChart {
    pub fn update(&mut self, data: &DashboardData, worker_id: WorkerId) {
        let end = SystemTime::now();
        let mut start = end - self.view_size;
        let mut times = vec![];

        while start <= end {
            times.push(start);
            start += Duration::from_secs(1);
        }

        self.task_records = times
            .into_iter()
            .map(|time| TaskCountRecord {
                time,
                count: data.query_task_count_for_worker(&worker_id, time),
            })
            .collect();
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let max_tasks_in_view = self
            .task_records
            .iter()
            .map(|record| record.count)
            .max()
            .unwrap_or(0) as f64;

        let task_counts: Vec<(f64, f64)> = self
            .task_records
            .iter()
            .map(|record| (get_time_as_secs(record.time) as f64, record.count as f64))
            .collect();
        let datasets = vec![Dataset::default()
            .name("tasks running")
            .marker(symbols::Marker::Braille)
            .style(Style::default().fg(Color::White))
            .data(&task_counts)];

        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title(Span::styled(
                        "Task Run History",
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
                    .bounds([0.0, max_tasks_in_view])
                    .labels(vec![
                        Span::from(0.to_string()),
                        Span::from(max_tasks_in_view.to_string()),
                    ]),
            );
        frame.render_widget(chart, rect);
    }
}

impl Default for WorkerTasksChart {
    fn default() -> Self {
        Self {
            task_records: vec![],
            view_size: Duration::from_secs(300),
        }
    }
}

fn get_time_as_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs()
}
