pub struct DashboardState {
    pub ui_state: DashboardScreen,
}

/**
 * What is currently being actively drawn on the dashboard
 */
pub enum DashboardScreen {
    WorkerHwMonitorScreen,
}

impl Default for DashboardState {
    fn default() -> Self {
        Self {
            ui_state: DashboardScreen::WorkerHwMonitorScreen, //default initial screen for the dashboard
        }
    }
}
