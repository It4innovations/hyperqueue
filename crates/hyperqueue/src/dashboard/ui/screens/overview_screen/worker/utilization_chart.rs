use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Local;
use tui::layout::{Alignment, Rect};
use tui::style::{Color, Modifier, Style};
use tui::symbols;
use tui::text::Span;
use tui::widgets::{Axis, Block, Borders, Chart, Dataset, GraphType};

use tako::hwstats::WorkerHwStateMessage;
use tako::worker::WorkerOverview;
use tako::WorkerId;

use crate::dashboard::data::DashboardData;
use crate::dashboard::data::{ItemWithTime, TimeRange};
use crate::dashboard::ui::styles::chart_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::utils::{get_average_cpu_usage_for_worker, get_memory_usage_pct};

pub struct WorkerUtilizationChart {
    range: TimeRange,
    overviews: Vec<ItemWithTime<WorkerOverview>>,
}

impl WorkerUtilizationChart {
    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        fn create_data<F: Fn(&WorkerHwStateMessage) -> f64>(
            overviews: &[ItemWithTime<WorkerOverview>],
            get_value: F,
        ) -> Vec<(f64, f64)> {
            overviews
                .iter()
                .map(|record| {
                    (
                        get_time_as_secs(record.time) as f64,
                        record
                            .item
                            .hw_state
                            .as_ref()
                            .map(|state| get_value(&state))
                            .unwrap_or(0.0),
                    )
                })
                .collect::<Vec<(f64, f64)>>()
        }
        fn create_dataset<'a>(items: &'a [(f64, f64)], name: &'a str, color: Color) -> Dataset<'a> {
            Dataset::default()
                .name(name)
                .marker(symbols::Marker::Dot)
                .style(Style::default().fg(color))
                .graph_type(GraphType::Line)
                .data(items)
        }

        let cpu_usage = create_data(&self.overviews, |state| {
            get_average_cpu_usage_for_worker(state)
        });
        let mem_usage = create_data(&self.overviews, |state| {
            get_memory_usage_pct(&state.state.memory_usage) as f64
        });

        let has_gpus = self.overviews.iter().any(|overview| {
            overview
                .item
                .hw_state
                .as_ref()
                .and_then(|state| {
                    state
                        .state
                        .nvidia_gpus
                        .as_ref()
                        .or(state.state.amd_gpus.as_ref())
                })
                .map(|stats| !stats.gpus.is_empty())
                .unwrap_or(false)
        });
        let mut gpu_usage = create_data(&self.overviews, |state| {
            let gpu_state = state
                .state
                .nvidia_gpus
                .as_ref()
                .or(state.state.amd_gpus.as_ref());
            gpu_state
                .map(|state| state.gpus[0].processor_usage as f64)
                .unwrap_or(0.0)
        });

        let mut datasets = vec![
            create_dataset(&cpu_usage, "avg cpu (%)", Color::Green),
            create_dataset(&mem_usage, "mem (%)", Color::Blue),
        ];
        if has_gpus {
            datasets.push(create_dataset(&gpu_usage, "gpu 0 (%)", Color::Red));
        }

        let chart = Chart::new(datasets)
            .style(chart_style_deselected())
            .block(
                Block::default()
                    .title(Span::styled(
                        "Utilization history",
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ))
                    .borders(Borders::ALL),
            )
            .x_axis(
                Axis::default()
                    .style(Style::default().fg(Color::Gray))
                    .bounds([
                        get_time_as_secs(self.range.start) as f64,
                        get_time_as_secs(self.range.end) as f64,
                    ])
                    .labels(vec![
                        format_time(self.range.start).into(),
                        format_time(self.range.end).into(),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::default().fg(Color::Gray))
                    .bounds([0.0, 100.0])
                    .labels((0..=10).map(|v| (v * 10).to_string().into()).collect())
                    .labels_alignment(Alignment::Right),
            );
        frame.render_widget(chart, rect);
    }

    pub fn update(&mut self, data: &DashboardData, worker_id: WorkerId) {
        let time_range = data.current_time_range();
        let overviews = data
            .workers()
            .get_worker_overviews_at(worker_id, time_range)
            .unwrap_or(&[]);
        self.overviews = overviews.into_iter().cloned().collect();
        self.range = time_range;
    }
}

impl Default for WorkerUtilizationChart {
    fn default() -> Self {
        Self {
            range: TimeRange::new(SystemTime::now(), SystemTime::now()),
            overviews: Default::default(),
        }
    }
}

fn get_time_as_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn format_time(time: SystemTime) -> String {
    let datetime: chrono::DateTime<Local> = time.into();
    datetime.format("%H:%M:%S").to_string()
}
