use crate::dashboard::events::DashboardEvent;
use tako::WorkerId;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Copy)]
pub enum ChangeScreenCommand {
    ClusterOverviewScreen,
    WorkerOverviewScreen(WorkerId),
}

pub struct ScreenController {
    event_channel: UnboundedSender<DashboardEvent>,
}

impl ScreenController {
    pub fn new(event_channel: UnboundedSender<DashboardEvent>) -> Self {
        Self { event_channel }
    }

    pub fn show_worker_screen(&mut self, worker_id: WorkerId) {
        self.send_change_command(ChangeScreenCommand::WorkerOverviewScreen(worker_id))
    }

    pub fn show_cluster_overview(&mut self) {
        self.send_change_command(ChangeScreenCommand::ClusterOverviewScreen)
    }

    fn send_change_command(&mut self, command: ChangeScreenCommand) {
        self.event_channel
            .send(DashboardEvent::ScreenChange(command))
            .expect("Failed to change screen")
    }
}
