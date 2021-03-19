use crate::scheduler::{ToSchedulerMessage};
use crate::messages::worker::ToWorkerMessage;
use crate::server::worker::WorkerId;

use crate::common::{Map, WrappedRcRefCell};
use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;
use crate::TaskId;
use crate::messages::gateway::{ToGatewayMessage, TaskUpdate, TaskState, TaskFailedMessage};
use crate::scheduler::protocol::TaskInfo;
use crate::messages::common::TaskFailInfo;

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
        error_info: TaskFailInfo,
    );
}

pub struct CommSender {
    workers: Map<WorkerId, UnboundedSender<Bytes>>,
    scheduler_sender: UnboundedSender<ToSchedulerMessage>,
    client_sender: UnboundedSender<ToGatewayMessage>,
}
pub type CommSenderRef = WrappedRcRefCell<CommSender>;

impl CommSenderRef {
    pub fn new(
        scheduler_sender: UnboundedSender<ToSchedulerMessage>,
        client_sender: UnboundedSender<ToGatewayMessage>,
    ) -> Self {
        WrappedRcRefCell::wrap(CommSender {
            workers: Default::default(),
            scheduler_sender,
            client_sender,
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
        log::debug!("Informing client about finished task={}", task_id);
        self.client_sender.send(ToGatewayMessage::TaskUpdate(TaskUpdate {
            id: task_id,
            state: TaskState::Finished
        }));
    }

    #[inline]
    fn send_client_task_removed(&mut self, task_id: TaskId) {
        //todo!()
        //self.gateway.send_client_task_removed(task_id);
    }

    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) {
        self.client_sender.send(ToGatewayMessage::TaskFailed({
            TaskFailedMessage {
                id: 0,
                info: error_info
            }
        }));

        /*self.gateway
            .send_client_task_error(task_id, consumers_id, error_info);*/
    }
}
