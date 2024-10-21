use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::cluster::worker::WorkerDetail;
use crate::dashboard::ui::terminal::DashboardFrame;
use crossterm::event::{KeyCode, KeyEvent};
use overview::JobOverview;
use ratatui::layout::Rect;

mod job_info_display;
mod job_tasks_chart;
mod jobs_table;
mod overview;

#[derive(Default)]
pub struct JobScreen {
    job_overview: JobOverview,
    task_worker_detail: WorkerDetail,

    active_fragment: ScreenState,
}

#[derive(Default)]
enum ScreenState {
    #[default]
    JobInfo,
    TaskWorkerDetail,
}

impl Screen for JobScreen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        match self.active_fragment {
            ScreenState::JobInfo => self.job_overview.draw(in_area, frame),
            ScreenState::TaskWorkerDetail => self.task_worker_detail.draw(in_area, frame),
        }
    }

    fn update(&mut self, data: &DashboardData) {
        match self.active_fragment {
            ScreenState::JobInfo => self.job_overview.update(data),
            ScreenState::TaskWorkerDetail => self.task_worker_detail.update(data),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        match self.active_fragment {
            ScreenState::JobInfo => self.job_overview.handle_key(key),
            ScreenState::TaskWorkerDetail => self.task_worker_detail.handle_key(key),
        }

        match key.code {
            KeyCode::Char('i') => {
                if let Some((_, selected_worker)) = self.job_overview.get_selected_task() {
                    self.task_worker_detail.set_worker_id(selected_worker);
                    self.active_fragment = ScreenState::TaskWorkerDetail;
                }
            }
            KeyCode::Backspace => {
                self.task_worker_detail.clear_worker_id();
                self.active_fragment = ScreenState::JobInfo
            }
            _ => {}
        }
    }
}
