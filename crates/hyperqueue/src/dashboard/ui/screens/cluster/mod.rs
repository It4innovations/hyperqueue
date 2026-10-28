use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::terminal::DashboardFrame;
use crossterm::event::{KeyCode, KeyEvent};
use numa::WorkerNumaScreen;
use overview::ClusterOverview;
use ratatui::layout::Rect;
use worker::WorkerDetail;

pub mod numa;
pub mod overview;
pub mod worker;

#[derive(Default)]
pub struct WorkerOverviewScreen {
    cluster_overview: ClusterOverview,
    worker_overview: WorkerDetail,
    worker_numa: WorkerNumaScreen,

    active_fragment: ScreenState,
}

#[derive(Default, PartialEq, Eq)]
enum ScreenState {
    #[default]
    ClusterOverview,
    WorkerInfo,
    WorkerNuma(NumaOrigin),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NumaOrigin {
    ClusterOverview,
    WorkerInfo,
}

impl Screen for WorkerOverviewScreen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        match self.active_fragment {
            ScreenState::ClusterOverview => self.cluster_overview.draw(in_area, frame),
            ScreenState::WorkerInfo => self.worker_overview.draw(in_area, frame),
            ScreenState::WorkerNuma(_) => self.worker_numa.draw(in_area, frame),
        }
    }

    fn update(&mut self, data: &DashboardData) {
        match self.active_fragment {
            ScreenState::ClusterOverview => self.cluster_overview.update(data),
            ScreenState::WorkerInfo => self.worker_overview.update(data),
            ScreenState::WorkerNuma(_) => self.worker_numa.update(data),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        match self.active_fragment {
            ScreenState::ClusterOverview => self.cluster_overview.handle_key(key),
            ScreenState::WorkerInfo => self.worker_overview.handle_key(key),
            ScreenState::WorkerNuma(_) => self.worker_numa.handle_key(key),
        }

        match key.code {
            KeyCode::Char('i')
                if self.active_fragment == ScreenState::ClusterOverview
                    && let Some(selected_worker) = self.cluster_overview.get_selected_worker() =>
            {
                self.worker_overview.set_worker_id(selected_worker);
                self.active_fragment = ScreenState::WorkerInfo;
            }
            KeyCode::Char('n') => match self.active_fragment {
                ScreenState::ClusterOverview => {
                    if let Some(selected_worker) = self.cluster_overview.get_selected_worker() {
                        self.worker_numa.set_worker_id(selected_worker);
                        self.active_fragment = ScreenState::WorkerNuma(NumaOrigin::ClusterOverview);
                    }
                }
                ScreenState::WorkerInfo => {
                    if let Some(worker_id) = self.worker_overview.get_worker_id() {
                        self.worker_numa.set_worker_id(worker_id);
                        self.active_fragment = ScreenState::WorkerNuma(NumaOrigin::WorkerInfo);
                    }
                }
                ScreenState::WorkerNuma(_) => {}
            },
            KeyCode::Backspace => match self.active_fragment {
                ScreenState::ClusterOverview => {}
                ScreenState::WorkerInfo => {
                    self.worker_overview.clear_worker_id();
                    self.active_fragment = ScreenState::ClusterOverview;
                }
                ScreenState::WorkerNuma(origin) => {
                    self.worker_numa.clear_worker_id();
                    match origin {
                        NumaOrigin::ClusterOverview => {
                            self.active_fragment = ScreenState::ClusterOverview;
                        }
                        NumaOrigin::WorkerInfo => {
                            self.active_fragment = ScreenState::WorkerInfo;
                        }
                    }
                }
            },
            _ => {}
        }
    }
}
