use tui::layout::{Constraint, Rect};
use tui::style::{Color, Modifier, Style};
use tui::symbols;
use tui::text::Span;
use tui::widgets::{Block, Borders, Chart, Dataset, GraphType};

use tako::hwstats::WorkerHwStateMessage;
use tako::worker::WorkerOverview;
use tako::WorkerId;

use crate::dashboard::data::DashboardData;
use crate::dashboard::data::{ItemWithTime, TimeRange};
use crate::dashboard::ui::styles::chart_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::chart::{get_time_as_secs, x_axis_time_chart, y_axis_steps};
use crate::dashboard::utils::{get_average_cpu_usage_for_worker, get_memory_usage_pct};

#[derive(Default)]
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
                .graph_type(GraphType::Scatter)
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
        let gpu_usage = create_data(&self.overviews, |state| {
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
            create_dataset(&cpu_usage, "CPU (avg) (%)", Color::Green),
            create_dataset(&mem_usage, "Mem (%)", Color::Blue),
        ];
        if has_gpus {
            datasets.push(create_dataset(&gpu_usage, "GPU 0 (%)", Color::Red));
        }

        let chart = Chart::new(datasets)
            .style(chart_style_deselected())
            .hidden_legend_constraints((Constraint::Ratio(1, 1), Constraint::Ratio(1, 1)))
            .block(
                Block::default()
                    .title(Span::styled(
                        "Utilization History",
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ))
                    .borders(Borders::ALL),
            )
            .x_axis(x_axis_time_chart(self.range))
            .y_axis(y_axis_steps(0.0, 100.0, 5));
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
