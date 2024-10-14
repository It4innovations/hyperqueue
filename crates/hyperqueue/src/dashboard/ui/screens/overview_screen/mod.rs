use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::overview_screen::overview::fragment::ClusterOverviewFragment;
use crate::dashboard::ui::screens::overview_screen::worker::fragment::WorkerOverviewFragment;
use crate::dashboard::ui::terminal::DashboardFrame;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;

pub mod overview;
pub mod worker;

#[derive(Default)]
pub struct WorkerOverviewScreen {
    cluster_overview: ClusterOverviewFragment,
    worker_overview: WorkerOverviewFragment,

    active_fragment: ScreenState,
}

#[derive(Default)]
enum ScreenState {
    #[default]
    ClusterOverview,
    WorkerInfo,
}

impl Screen for WorkerOverviewScreen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        match self.active_fragment {
            ScreenState::ClusterOverview => self.cluster_overview.draw(in_area, frame),
            ScreenState::WorkerInfo => self.worker_overview.draw(in_area, frame),
        }
    }

    fn update(&mut self, data: &DashboardData) {
        match self.active_fragment {
            ScreenState::ClusterOverview => self.cluster_overview.update(data),
            ScreenState::WorkerInfo => self.worker_overview.update(data),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        match self.active_fragment {
            ScreenState::ClusterOverview => self.cluster_overview.handle_key(key),
            ScreenState::WorkerInfo => self.worker_overview.handle_key(key),
        }

        match key.code {
            KeyCode::Char('i') => {
                if let Some(selected_worker) = self.cluster_overview.get_selected_worker() {
                    self.worker_overview.set_worker_id(selected_worker);
                    self.active_fragment = ScreenState::WorkerInfo;
                }
            }
            KeyCode::Backspace => {
                self.worker_overview.clear_worker_id();
                self.active_fragment = ScreenState::ClusterOverview
            }
            _ => {}
        }
    }
}
