use crate::dashboard::data::timelines::job_timeline::DashboardTaskState;
use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::chart::{
    get_time_as_secs, ChartPlotter, DashboardChart, PlotStyle,
};
use crate::JobId;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::symbols::Marker;
use std::time::SystemTime;
use tako::Map;

const RUNNING_TAG: &str = "Running";
const FINISHED_TAG: &str = "Finished";
const FAILED_TAG: &str = "Failed";

#[derive(Default)]
pub struct JobTaskChart {
    /// The Job for which we want to see the tasks for.
    job_id: Option<JobId>,
    /// The chart.
    chart: DashboardChart,
}

impl JobTaskChart {
    /// Sets a new job id for the chart to plot data for.
    pub fn set_job_id(&mut self, job_id: JobId) {
        match self.job_id {
            Some(chart_job_id) if chart_job_id == job_id => (),
            _ => {
                self.job_id = Some(job_id);
                self.chart = Default::default();
                let tasks_count_plotter: Box<dyn ChartPlotter> =
                    Box::new(TaskCountsPlotter(job_id));
                self.chart.add_chart_plotter(tasks_count_plotter);
            }
        }
    }

    pub fn clear_chart(&mut self) {
        self.job_id = None;
        self.chart = Default::default();
    }
}

impl JobTaskChart {
    pub fn update(&mut self, data: &DashboardData) {
        if let Some(job_id) = self.job_id {
            let (num_running_tasks, num_failed_tasks, num_finished_tasks) =
                get_task_counts_for_job(job_id, data, SystemTime::now());
            self.chart.set_chart_name(
                format!("Running: {num_running_tasks}, Finished: {num_finished_tasks}, Failed: {num_failed_tasks}").as_str(),
            );
        } else {
            self.chart
                .set_chart_name("Please select a Job to show Task counts");
        }
        self.chart.update(data);
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.chart.draw(rect, frame);
    }
}

struct TaskCountsPlotter(JobId);

impl ChartPlotter for TaskCountsPlotter {
    fn get_charts(&self) -> Map<String, PlotStyle> {
        let mut plots_and_styles: Map<String, PlotStyle> = Default::default();

        let running_points_style = PlotStyle {
            color: Color::Yellow,
            marker: Marker::Braille,
        };
        let finished_points_style = PlotStyle {
            color: Color::Green,
            marker: Marker::Braille,
        };
        let failed_points_style = PlotStyle {
            color: Color::Red,
            marker: Marker::Braille,
        };

        plots_and_styles.insert(RUNNING_TAG.to_string(), running_points_style);
        plots_and_styles.insert(FINISHED_TAG.to_string(), finished_points_style);
        plots_and_styles.insert(FAILED_TAG.to_string(), failed_points_style);

        plots_and_styles
    }

    /// Fetches the number of Running/Finished/Failed tasks at a given time and makes it available to the chart.
    fn fetch_data_points(
        &self,
        data: &DashboardData,
        at_time: SystemTime,
    ) -> Map<String, (f64, f64)> {
        let mut data_points: Map<String, (f64, f64)> = Default::default();

        let (num_running_tasks, num_failed_tasks, num_finished_tasks) =
            get_task_counts_for_job(self.0, data, at_time);
        if num_running_tasks != 0 {
            data_points.insert(
                RUNNING_TAG.to_string(),
                (get_time_as_secs(at_time), num_running_tasks as f64),
            );
        }
        if num_finished_tasks != 0 {
            data_points.insert(
                FINISHED_TAG.to_string(),
                (get_time_as_secs(at_time), num_finished_tasks as f64),
            );
        }
        if num_failed_tasks != 0 {
            data_points.insert(
                FAILED_TAG.to_string(),
                (get_time_as_secs(at_time), num_failed_tasks as f64),
            );
        }

        data_points
    }
}

/// Fetches the count of tasks for a `job` as (`running_tasks`, `failed_tasks`, `finished_tasks`)
fn get_task_counts_for_job(
    job_id: JobId,
    data: &DashboardData,
    at_time: SystemTime,
) -> (u32, u32, u32) {
    data.query_task_history_for_job(job_id, at_time)
        .filter_map(|(_, info)| info.get_task_state_at(at_time))
        .fold(
            (0, 0, 0),
            |(running, failed, finished), state| match state {
                DashboardTaskState::Running => (running + 1, failed, finished),
                DashboardTaskState::Failed => (running, failed + 1, finished),
                DashboardTaskState::Finished => (running, failed, finished + 1),
            },
        )
}
