use crate::dashboard::data::timelines::job_timeline::DashboardTaskState;
use crate::dashboard::data::{DashboardData, ItemWithTime, TimeRange};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::chart::{
    create_chart, create_dataset, generate_dataset_entries, generate_time_data, RangeSteps,
};
use ratatui::layout::Rect;
use ratatui::style::Color;
use std::time::SystemTime;
use tako::JobId;

#[derive(Default)]
pub struct JobTaskChart {
    /// The job for which we want to see the tasks chart.
    job_id: Option<JobId>,
    stats: Option<Vec<ItemWithTime<TaskStats>>>,
    range: TimeRange,
}

impl JobTaskChart {
    /// Sets a new job id for the chart to plot data for.
    pub fn set_job_id(&mut self, job_id: JobId) {
        self.job_id = Some(job_id);
    }

    pub fn unset_job_id(&mut self) {
        self.job_id = None;
        self.stats = None;
    }
}

impl JobTaskChart {
    pub fn update(&mut self, data: &DashboardData) {
        self.range = data.current_time_range();
        if let Some(job_id) = self.job_id {
            let steps = RangeSteps::new(self.range, 25);
            self.stats = Some(generate_time_data(steps, |time| {
                get_task_counts_for_job(job_id, data, time)
            }));
        } else {
            self.stats = None;
        }
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        match &self.stats {
            Some(entries) => {
                let Some(stats) = entries.last() else {
                    return;
                };
                let title = format!(
                    "Running: {}, Finished: {}, Failed: {}",
                    stats.item.running, stats.item.finished, stats.item.failed
                );

                let running = generate_dataset_entries(entries, |stats| stats.running as f64);
                let finished = generate_dataset_entries(entries, |stats| stats.finished as f64);
                let failed = generate_dataset_entries(entries, |stats| stats.failed as f64);

                let datasets = vec![
                    create_dataset(&running, "Running", Color::Yellow),
                    create_dataset(&finished, "Finished", Color::Green),
                    create_dataset(&failed, "Failed", Color::Red),
                ];
                let chart = create_chart(datasets, &title, self.range);
                frame.render_widget(chart, rect);
            }
            None => {
                let chart = create_chart(
                    vec![],
                    "Please select a Job to show Task counts",
                    self.range,
                );
                frame.render_widget(chart, rect);
            }
        };
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct TaskStats {
    running: u64,
    failed: u64,
    finished: u64,
}

impl TaskStats {
    fn update(self, state: DashboardTaskState) -> Self {
        let TaskStats {
            mut running,
            mut failed,
            mut finished,
        } = self;
        match state {
            DashboardTaskState::Running => {
                running += 1;
            }
            DashboardTaskState::Finished => {
                finished += 1;
            }
            DashboardTaskState::Failed => {
                failed += 1;
            }
        }
        Self {
            running,
            failed,
            finished,
        }
    }
}

/// Fetches the count of tasks for a `job` as (`running_tasks`, `failed_tasks`, `finished_tasks`)
fn get_task_counts_for_job(job_id: JobId, data: &DashboardData, at_time: SystemTime) -> TaskStats {
    data.query_task_history_for_job(job_id, at_time)
        .filter_map(|(_, info)| info.get_task_state_at(at_time))
        .fold(TaskStats::default(), |acc, state| acc.update(state))
}
