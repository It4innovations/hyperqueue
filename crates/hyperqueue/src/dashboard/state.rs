use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::home::screen::ClusterOverviewScreen;
use crate::dashboard::ui::screens::worker::screen::WorkerOverviewScreen;
use std::ops::DerefMut;
use tako::common::WrappedRcRefCell;
use tako::WorkerId;

pub struct DashboardState {
    cluster_overview_screen: Box<ClusterOverviewScreen>,
    worker_info_screen: Box<WorkerOverviewScreen>,

    current_screen: DashboardScreen,
    data_source: WrappedRcRefCell<DashboardData>,
}

pub enum DashboardScreen {
    ClusterOverviewScreen,
    WorkerOverviewScreen(Option<WorkerId>),
}

impl DashboardState {
    pub fn new(data_source: DashboardData) -> Self {
        Self {
            data_source: WrappedRcRefCell::wrap(data_source),
            cluster_overview_screen: Box::new(ClusterOverviewScreen::default()),
            worker_info_screen: Box::new(WorkerOverviewScreen::default()),
            current_screen: DashboardScreen::ClusterOverviewScreen,
        }
    }

    pub fn change_screen(&mut self, screen: DashboardScreen) {
        self.current_screen = screen;
    }

    pub fn get_data_source(&self) -> &WrappedRcRefCell<DashboardData> {
        &self.data_source
    }

    pub fn get_current_screen_mut(&mut self) -> &mut dyn Screen {
        match self.current_screen {
            DashboardScreen::ClusterOverviewScreen => self.cluster_overview_screen.deref_mut(),
            DashboardScreen::WorkerOverviewScreen(worker_id) => {
                let screen = self.worker_info_screen.deref_mut();
                //todo: fix this optional
                if let Some(id) = worker_id {
                    screen.set_worker_id(id);
                }
                screen
            }
        }
    }
}
