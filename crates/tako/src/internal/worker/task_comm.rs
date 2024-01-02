use crate::launcher::StopReason;
use tokio::sync::oneshot::Sender;

/// Allows communicating with a task currently running on the worker.
pub struct RunningTaskComm {
    stop_sender: Option<Sender<StopReason>>,
}

impl RunningTaskComm {
    pub fn new(stop_sender: Sender<StopReason>) -> Self {
        Self {
            stop_sender: Some(stop_sender),
        }
    }

    pub fn cancel_task(&mut self) {
        self.send_stop(StopReason::Cancel);
    }

    pub fn timeout_task(&mut self) {
        self.send_stop(StopReason::Timeout);
    }

    fn send_stop(&mut self, reason: StopReason) {
        if let Some(sender) = std::mem::take(&mut self.stop_sender) {
            assert!(sender.send(reason).is_ok());
        } else {
            log::debug!("Stopping a task in stopping process");
        }
    }
}
