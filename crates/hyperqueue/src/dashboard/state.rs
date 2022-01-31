use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::controller::{ChangeScreenCommand, ScreenController};
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::home::screen::ClusterOverviewScreen;
use crate::dashboard::ui::screens::worker::screen::WorkerOverviewScreen;
use tako::common::WrappedRcRefCell;

pub struct DashboardState {
    cluster_overview_screen: ClusterOverviewScreen,
    worker_overview_screen: WorkerOverviewScreen,

    data_source: WrappedRcRefCell<DashboardData>,

    current_screen: DashboardScreenState,
    controller: ScreenController,
}

enum DashboardScreenState {
    ClusterOverviewScreen,
    WorkerOverviewScreen,
}

impl DashboardState {
    pub fn new(data_source: DashboardData, controller: ScreenController) -> Self {
        Self {
            data_source: WrappedRcRefCell::wrap(data_source),
            cluster_overview_screen: ClusterOverviewScreen::default(),
            worker_overview_screen: WorkerOverviewScreen::default(),
            current_screen: DashboardScreenState::ClusterOverviewScreen,
            controller,
        }
    }

    pub fn get_data_source(&self) -> &WrappedRcRefCell<DashboardData> {
        &self.data_source
    }

    /// Updates data for the next screen if required and changes the dashboard state to show it.
    pub fn change_current_screen(&mut self, next_screen: ChangeScreenCommand) {
        match next_screen {
            ChangeScreenCommand::ClusterOverviewScreen => {
                self.current_screen = DashboardScreenState::ClusterOverviewScreen;
            }
            ChangeScreenCommand::WorkerOverviewScreen(worker_id) => {
                self.worker_overview_screen.set_worker_id(worker_id);
                self.current_screen = DashboardScreenState::WorkerOverviewScreen;
            }
        }
    }

    pub fn get_current_screen_and_controller(
        &mut self,
    ) -> (&mut dyn Screen, &mut ScreenController) {
        match self.current_screen {
            DashboardScreenState::ClusterOverviewScreen => {
                (&mut self.cluster_overview_screen, &mut self.controller)
            }
            DashboardScreenState::WorkerOverviewScreen => {
                (&mut self.worker_overview_screen, &mut self.controller)
            }
        }
    }
}
