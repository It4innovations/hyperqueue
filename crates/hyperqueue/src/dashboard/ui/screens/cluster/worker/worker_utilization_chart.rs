use ratatui::layout::Rect;
use ratatui::style::Color;

use tako::WorkerId;
use tako::worker::WorkerOverview;

use crate::dashboard::data::DashboardData;
use crate::dashboard::data::{ItemWithTime, TimeRange};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::chart::y_axis_steps;
use crate::dashboard::ui::widgets::chart::{
    create_count_chart, create_dataset, generate_dataset_entries,
};
use crate::dashboard::utils::{get_average_cpu_usage_for_worker, get_memory_usage_pct};

#[derive(Default)]
pub struct WorkerUtilizationChart {
    range: TimeRange,
    overviews: Vec<ItemWithTime<WorkerOverview>>,
}

impl WorkerUtilizationChart {
    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let cpu_usage = generate_dataset_entries(&self.overviews, |overview| {
            overview
                .hw_state
                .as_ref()
                .map(get_average_cpu_usage_for_worker)
                .unwrap_or(0.0)
        });
        let mem_usage = generate_dataset_entries(&self.overviews, |overview| {
            overview
                .hw_state
                .as_ref()
                .map(|s| get_memory_usage_pct(&s.state.memory_usage) as f64)
                .unwrap_or(0.0)
        });

        let gpu_usage = generate_dataset_entries(&self.overviews, |overview| {
            let Some(hw_state) = overview.hw_state.as_ref().map(|s| &s.state) else {
                return 0.0;
            };
            let gpu_usages: Vec<f64> = hw_state
                .nvidia_gpus
                .as_ref()
                .into_iter()
                .chain(hw_state.amd_gpus.as_ref())
                .flat_map(|stats| &stats.gpus)
                .map(|gpu| gpu.processor_usage as f64)
                .collect();
            if !gpu_usages.is_empty() {
                gpu_usages.iter().sum::<f64>() / gpu_usages.len() as f64
            } else {
                0.0
            }
        });
        let has_gpus = gpu_usage.iter().any(|(_, usage)| *usage > 0.0);

        let mut datasets = vec![
            create_dataset(&cpu_usage, "CPU (avg) (%)", Color::Green),
            create_dataset(&mem_usage, "Mem (%)", Color::Blue),
        ];
        if has_gpus {
            datasets.push(create_dataset(&gpu_usage, "GPU (avg) (%)", Color::Red));
        }

        let chart = create_count_chart(datasets, "Utilization History", self.range)
            .y_axis(y_axis_steps(0.0, 100.0, 5));
        frame.render_widget(chart, rect);
    }

    pub fn update(&mut self, data: &DashboardData, worker_id: WorkerId) {
        let time_range = data.current_time_range();
        self.overviews = data
            .workers()
            .query_worker_overviews_at(worker_id, time_range)
            .unwrap_or(&[])
            .to_vec();
        self.range = time_range;
    }
}
