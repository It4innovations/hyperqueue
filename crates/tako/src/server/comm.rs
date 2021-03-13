use crate::scheduler::{ToSchedulerMessage};
use crate::messages::worker::ToWorkerMessage;
use crate::server::worker::WorkerId;

use crate::common::{Map, WrappedRcRefCell};
use crate::server::gateway::Gateway;
use crate::server::task::ErrorInfo;
use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;
use crate::TaskId;

pub trait Comm {
    fn send_worker_message(&mut self, worker_id: WorkerId, message: &ToWorkerMessage);
    fn broadcast_worker_message(&mut self, message: &ToWorkerMessage);
    fn send_scheduler_message(&mut self, message: ToSchedulerMessage);

    fn send_client_task_finished(&mut self, task_id: TaskId);
    fn send_client_task_removed(&mut self, task_id: TaskId);
    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: ErrorInfo,
    );
}

pub struct CommSender {
    workers: Map<WorkerId, UnboundedSender<Bytes>>,
    scheduler_sender: UnboundedSender<ToSchedulerMessage>,
    pub gateway: Box<dyn Gateway>,
}
pub type CommSenderRef = WrappedRcRefCell<CommSender>;

impl CommSenderRef {
    pub fn new(
        scheduler_sender: UnboundedSender<ToSchedulerMessage>,
        gateway: Box<dyn Gateway>,
    ) -> Self {
        WrappedRcRefCell::wrap(CommSender {
            workers: Default::default(),
            scheduler_sender,
            gateway,
        })
    }
}

impl CommSender {
    pub fn add_worker(&mut self, worker_id: WorkerId, sender: UnboundedSender<Bytes>) {
        assert!(self.workers.insert(worker_id, sender).is_none());
    }
}

impl Comm for CommSender {
    fn send_worker_message(&mut self, worker_id: WorkerId, message: &ToWorkerMessage) {
        let data = rmp_serde::to_vec_named(&message).unwrap();
        self.workers
            .get(&worker_id)
            .unwrap()
            .send(data.into())
            .expect("Send to worker failed");
    }

    fn broadcast_worker_message(&mut self, message: &ToWorkerMessage) {
        let data: Bytes = rmp_serde::to_vec_named(&message).unwrap().into();
        for sender in self.workers.values() {
            sender.send(data.clone()).expect("Send to worker failed");
        }
    }

    #[inline]
    fn send_scheduler_message(&mut self, message: ToSchedulerMessage) {
        self.scheduler_sender
            .send(message)
            .expect("Sending scheduler message failed");
    }

    #[inline]
    fn send_client_task_finished(&mut self, task_id: TaskId) {
        self.gateway.send_client_task_finished(task_id);
    }

    #[inline]
    fn send_client_task_removed(&mut self, task_id: TaskId) {
        self.gateway.send_client_task_removed(task_id);
    }

    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: ErrorInfo,
    ) {
        self.gateway
            .send_client_task_error(task_id, consumers_id, error_info);
    }
}
