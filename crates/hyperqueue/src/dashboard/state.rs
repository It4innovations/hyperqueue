use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::home::screen::ClusterOverviewScreen;
use crate::dashboard::ui::screens::worker::screen::WorkerOverviewScreen;
use std::ops::DerefMut;
use tako::common::WrappedRcRefCell;
use tako::WorkerId;

pub struct DashboardState {
    screen_controller: WrappedRcRefCell<ScreenController>,
    cluster_overview_screen: Box<ClusterOverviewScreen>,
    worker_info_screen: Box<WorkerOverviewScreen>,

    data_source: WrappedRcRefCell<DashboardData>,
}

pub enum DashboardScreen {
    ClusterOverviewScreen,
    WorkerOverviewScreen(Option<WorkerId>),
}

pub struct ScreenController {
    pub current_screen: DashboardScreen,
}

impl DashboardState {
    pub fn new(data_source: DashboardData, controller: WrappedRcRefCell<ScreenController>) -> Self {
        Self {
            data_source: WrappedRcRefCell::wrap(data_source),
            cluster_overview_screen: Box::new(ClusterOverviewScreen::new(controller.clone())),
            worker_info_screen: Box::new(WorkerOverviewScreen::new(controller.clone())),
            screen_controller: controller,
        }
    }

    pub fn change_screen(&mut self, screen: DashboardScreen) {
        self.screen_controller.get_mut().change_screen(screen);
    }

    pub fn get_data_source(&self) -> &WrappedRcRefCell<DashboardData> {
        &self.data_source
    }

    pub fn get_current_screen_mut(&mut self) -> &mut dyn Screen {
        match self.screen_controller.get_mut().current_screen {
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

impl ScreenController {
    pub fn change_screen(&mut self, to_screen: DashboardScreen) {
        self.current_screen = to_screen;
    }
}

impl Default for ScreenController {
    fn default() -> Self {
        Self {
            current_screen: DashboardScreen::ClusterOverviewScreen,
        }
    }
}
