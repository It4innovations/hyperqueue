use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::controller::{ChangeScreenCommand, ScreenController};
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::auto_allocator::screen::AutoAllocatorScreen;
use crate::dashboard::ui::screens::home::screen::ClusterOverviewScreen;
use crate::dashboard::ui::screens::job::screen::JobScreen;
use crate::dashboard::ui::screens::worker::screen::WorkerOverviewScreen;
use tako::common::WrappedRcRefCell;

pub struct DashboardState {
    cluster_overview_screen: ClusterOverviewScreen,
    worker_overview_screen: WorkerOverviewScreen,
    auto_allocator_screen: AutoAllocatorScreen,
    job_overview_screen: JobScreen,

    data_source: WrappedRcRefCell<DashboardData>,

    current_screen: DashboardScreenState,
    controller: ScreenController,
}

enum DashboardScreenState {
    ClusterOverview,
    WorkerOverview,
    AutoAllocator,
    JobOverview,
}

impl DashboardState {
    pub fn new(data_source: DashboardData, controller: ScreenController) -> Self {
        Self {
            data_source: WrappedRcRefCell::wrap(data_source),
            cluster_overview_screen: ClusterOverviewScreen::default(),
            worker_overview_screen: WorkerOverviewScreen::default(),
            auto_allocator_screen: Default::default(),
            job_overview_screen: Default::default(),
            current_screen: DashboardScreenState::ClusterOverview,
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
                self.current_screen = DashboardScreenState::ClusterOverview;
            }
            ChangeScreenCommand::WorkerOverviewScreen(worker_id) => {
                self.worker_overview_screen.set_worker_id(worker_id);
                self.current_screen = DashboardScreenState::WorkerOverview;
            }
            ChangeScreenCommand::AutoAllocatorScreen => {
                self.current_screen = DashboardScreenState::AutoAllocator;
            }
            ChangeScreenCommand::JobOverviewScreen => {
                self.current_screen = DashboardScreenState::JobOverview;
            }
        }
    }

    pub fn get_current_screen_and_controller(
        &mut self,
    ) -> (&mut dyn Screen, &mut ScreenController) {
        match self.current_screen {
            DashboardScreenState::ClusterOverview => {
                (&mut self.cluster_overview_screen, &mut self.controller)
            }
            DashboardScreenState::WorkerOverview => {
                (&mut self.worker_overview_screen, &mut self.controller)
            }
            DashboardScreenState::AutoAllocator => {
                (&mut self.auto_allocator_screen, &mut self.controller)
            }
            DashboardScreenState::JobOverview => {
                (&mut self.job_overview_screen, &mut self.controller)
            }
        }
    }
}
