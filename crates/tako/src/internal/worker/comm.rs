use crate::comm::serialize;
use crate::internal::messages::worker::FromWorkerMessage;
use bytes::Bytes;
use std::rc::Rc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

pub(crate) trait WorkerComm {}

pub(crate) struct RealWorkerComm {
    sender: Option<UnboundedSender<Bytes>>,
    start_task_notify: Rc<Notify>,
    worker_is_empty_notify: Option<Rc<Notify>>,
}

impl RealWorkerComm {
    pub fn new(sender: UnboundedSender<Bytes>, start_task_notify: Rc<Notify>) -> Self {
        RealWorkerComm {
            sender: Some(sender),
            start_task_notify,
            worker_is_empty_notify: None,
        }
    }

    pub fn send_message_to_server(&self, message: FromWorkerMessage) {
        if let Some(sender) = self.sender.as_ref() {
            if sender.send(serialize(&message).unwrap().into()).is_err() {
                log::debug!("Message could not be sent to server");
            }
        } else {
            log::debug!(
                "Attempting to send a message to server, but server has already disconnected"
            );
        }
    }

    pub fn drop_sender(&mut self) {
        self.sender = None;
    }
}
