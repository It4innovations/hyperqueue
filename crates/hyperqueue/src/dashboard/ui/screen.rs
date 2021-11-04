use tako::messages::gateway::CollectedOverview;
use termion::event::Key;

use crate::dashboard::ui::terminal::DashboardFrame;
use crate::transfer::messages::{JobDetail, WorkerInfo};

pub trait Screen {
    fn draw(&mut self, frame: &mut DashboardFrame);

    // Note: might be generalized in the future to allow the screen to fetch custom data by itself
    fn update(&mut self, state: ClusterState);

    fn handle_key(&mut self, key: Key);
}

pub struct ClusterState {
    pub overview: CollectedOverview,
    pub worker_info: Vec<WorkerInfo>,
    pub worker_jobs_info: Vec<(u32, JobDetail)>,
}
