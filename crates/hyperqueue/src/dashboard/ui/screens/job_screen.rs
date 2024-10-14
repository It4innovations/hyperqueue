use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::job::fragment::JobFragment;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::overview_screen::worker::fragment::WorkerOverviewFragment;
use crate::dashboard::ui::terminal::DashboardFrame;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;

pub struct JobScreen {
    job_overview_fragment: JobFragment,
    task_runner_worker: WorkerOverviewFragment,

    active_fragment: ScreenState,
}

enum ScreenState {
    JobInfo,
    TaskWorkerDetail,
}

impl Screen for JobScreen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        match self.active_fragment {
            ScreenState::JobInfo => self.job_overview_fragment.draw(in_area, frame),
            ScreenState::TaskWorkerDetail => self.task_runner_worker.draw(in_area, frame),
        }
    }

    fn update(&mut self, data: &DashboardData) {
        match self.active_fragment {
            ScreenState::JobInfo => self.job_overview_fragment.update(data),
            ScreenState::TaskWorkerDetail => self.task_runner_worker.update(data),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        match self.active_fragment {
            ScreenState::JobInfo => self.job_overview_fragment.handle_key(key),
            ScreenState::TaskWorkerDetail => self.task_runner_worker.handle_key(key),
        }

        match key.code {
            KeyCode::Char('i') => {
                if let Some((_, selected_worker)) = self.job_overview_fragment.get_selected_task() {
                    self.task_runner_worker.set_worker_id(selected_worker);
                    self.active_fragment = ScreenState::TaskWorkerDetail;
                }
            }
            KeyCode::Backspace => {
                self.task_runner_worker.clear_worker_id();
                self.active_fragment = ScreenState::JobInfo
            }
            _ => {}
        }
    }
}

impl Default for JobScreen {
    fn default() -> Self {
        JobScreen {
            job_overview_fragment: Default::default(),
            task_runner_worker: Default::default(),
            active_fragment: ScreenState::JobInfo,
        }
    }
}
