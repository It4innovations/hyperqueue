use crate::dashboard::events::DashboardEvent;
use tako::WorkerId;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Copy)]
pub enum ChangeScreenCommand {
    ClusterOverviewScreen,
    JobOverviewScreen,
    WorkerOverviewScreen(WorkerId),
    AutoAllocatorScreen,
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

    pub fn show_job_screen(&mut self) {
        self.send_change_command(ChangeScreenCommand::JobOverviewScreen)
    }

    pub fn show_cluster_overview(&mut self) {
        self.send_change_command(ChangeScreenCommand::ClusterOverviewScreen)
    }

    pub fn show_auto_allocator_screen(&mut self) {
        self.send_change_command(ChangeScreenCommand::AutoAllocatorScreen)
    }

    fn send_change_command(&mut self, command: ChangeScreenCommand) {
        self.event_channel
            .send(DashboardEvent::ScreenChange(command))
            .expect("Failed to change screen")
    }
}
