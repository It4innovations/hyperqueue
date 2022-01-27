use crate::dashboard::data::DashboardData;
use crate::dashboard::events::DashboardEvent;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::home::screen::ClusterOverviewScreen;
use crate::dashboard::ui::screens::worker::screen::WorkerOverviewScreen;
use std::ops::DerefMut;
use tako::common::WrappedRcRefCell;
use tako::WorkerId;
use tokio::sync::mpsc::UnboundedSender;

pub struct DashboardState {
    cluster_overview_screen: Box<ClusterOverviewScreen>,
    worker_info_screen: Box<WorkerOverviewScreen>,

    data_source: WrappedRcRefCell<DashboardData>,
    current_screen: DashboardScreenState,
}

impl DashboardState {
    pub fn new(data_source: DashboardData, switcher: UnboundedSender<DashboardEvent>) -> Self {
        Self {
            data_source: WrappedRcRefCell::wrap(data_source),
            cluster_overview_screen: Box::new(ClusterOverviewScreen::new(switcher.clone())),
            worker_info_screen: Box::new(WorkerOverviewScreen::new(switcher)),
            current_screen: DashboardScreenState::ClusterOverviewScreen,
        }
    }

    pub fn get_data_source(&self) -> &WrappedRcRefCell<DashboardData> {
        &self.data_source
    }

    pub fn get_current_screen_mut(&mut self) -> &mut dyn Screen {
        match self.current_screen {
            DashboardScreenState::ClusterOverviewScreen => self.cluster_overview_screen.deref_mut(),
            DashboardScreenState::WorkerOverviewScreen(worker_id) => {
                let screen = self.worker_info_screen.deref_mut();
                screen.set_worker_id(worker_id);
                screen
            }
        }
    }

    pub fn switch_screen(&mut self, to_screen: DashboardScreenState) {
        self.current_screen = to_screen;
    }
}

#[derive(Clone, Copy)]
pub enum DashboardScreenState {
    ClusterOverviewScreen,
    WorkerOverviewScreen(WorkerId),
}
