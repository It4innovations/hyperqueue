use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::overview::fragment::ClusterOverviewFragment;
use crate::dashboard::ui::fragments::worker::fragment::WorkerOverviewFragment;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::terminal::DashboardFrame;
use termion::event::Key;
use tui::layout::Rect;

pub struct OverviewScreen {
    cluster_overview: ClusterOverviewFragment,
    worker_overview: WorkerOverviewFragment,

    active_fragment: ScreenState,
}

enum ScreenState {
    ClusterOverview,
    WorkerInfo,
}

impl Screen for OverviewScreen {
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

    fn handle_key(&mut self, key: Key) {
        match self.active_fragment {
            ScreenState::ClusterOverview => self.cluster_overview.handle_key(key),
            ScreenState::WorkerInfo => self.worker_overview.handle_key(key),
        }

        match key {
            Key::Char('i') => {
                if let Some(selected_worker) = self.cluster_overview.get_selected_worker() {
                    self.worker_overview.set_worker_id(selected_worker);
                    self.active_fragment = ScreenState::WorkerInfo;
                }
            }
            Key::Backspace => {
                self.worker_overview.clear_worker_id();
                self.active_fragment = ScreenState::ClusterOverview
            }
            _ => {}
        }
    }
}

impl Default for OverviewScreen {
    fn default() -> Self {
        OverviewScreen {
            cluster_overview: Default::default(),
            worker_overview: Default::default(),
            active_fragment: ScreenState::ClusterOverview,
        }
    }
}
