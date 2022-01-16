use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

use crate::common::{Map, WrappedRcRefCell};
use crate::messages::common::{TaskFailInfo, WorkerConfiguration};
use crate::messages::gateway::{
    LostWorkerMessage, LostWorkerReason, NewWorkerMessage, TaskFailedMessage, TaskState,
    TaskUpdate, ToGatewayMessage,
};
use crate::messages::worker::ToWorkerMessage;
use crate::server::system::TaskSystem;
use crate::server::task::SerializedTaskContext;
use crate::transfer::auth::serialize;
use crate::{TaskId, WorkerId};

pub trait Comm {
    fn send_worker_message<System: TaskSystem>(
        &mut self,
        worker_id: WorkerId,
        message: &ToWorkerMessage<System>,
    );
    fn broadcast_worker_message<System: TaskSystem>(&mut self, message: &ToWorkerMessage<System>);
    fn ask_for_scheduling(&mut self);

    fn send_client_task_finished(&mut self, task_id: TaskId);
    fn send_client_task_started(
        &mut self,
        task_id: TaskId,
        worker_id: WorkerId,
        context: SerializedTaskContext,
    );
    //fn send_client_task_removed(&mut self, task_id: TaskId);
    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: TaskFailInfo,
    );

    fn send_client_worker_new(&mut self, worker_id: WorkerId, configuration: &WorkerConfiguration);
    fn send_client_worker_lost(
        &mut self,
        worker_id: WorkerId,
        running_tasks: Vec<TaskId>,
        reason: LostWorkerReason,
    );
}

pub struct CommSender {
    workers: Map<WorkerId, UnboundedSender<Bytes>>,
    need_scheduling: bool,
    scheduler_wakeup: Rc<Notify>,
    client_sender: UnboundedSender<ToGatewayMessage>,
    panic_on_worker_lost: bool,
}

pub type CommSenderRef = WrappedRcRefCell<CommSender>;

impl CommSenderRef {
    pub fn new(
        scheduler_wakeup: Rc<Notify>,
        client_sender: UnboundedSender<ToGatewayMessage>,
        panic_on_worker_lost: bool,
    ) -> Self {
        WrappedRcRefCell::wrap(CommSender {
            workers: Default::default(),
            scheduler_wakeup,
            client_sender,
            need_scheduling: false,
            panic_on_worker_lost,
        })
    }
}

impl CommSender {
    pub fn add_worker(&mut self, worker_id: WorkerId, sender: UnboundedSender<Bytes>) {
        assert!(self.workers.insert(worker_id, sender).is_none());
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) {
        if self.panic_on_worker_lost {
            panic!("Worker lost while server is running in testing mode with flag '--panic-on-worker-lost'");
        }
        assert!(self.workers.remove(&worker_id).is_some());
    }

    #[inline]
    pub fn reset_scheduling_flag(&mut self) {
        self.need_scheduling = false
    }
}

impl Comm for CommSender {
    fn send_worker_message<System: TaskSystem>(
        &mut self,
        worker_id: WorkerId,
        message: &ToWorkerMessage<System>,
    ) {
        let data = serialize(&message).unwrap();
        self.workers
            .get(&worker_id)
            .unwrap()
            .send(data.into())
            .expect("Send to worker failed");
    }

    fn broadcast_worker_message<System: TaskSystem>(&mut self, message: &ToWorkerMessage<System>) {
        let data: Bytes = serialize(&message).unwrap().into();
        for sender in self.workers.values() {
            sender.send(data.clone()).expect("Send to worker failed");
        }
    }

    #[inline]
    fn ask_for_scheduling(&mut self) {
        if !self.need_scheduling {
            self.need_scheduling = true;
            self.scheduler_wakeup.notify_one();
        }
    }

    #[inline]
    fn send_client_task_finished(&mut self, task_id: TaskId) {
        log::debug!("Informing client about finished task={}", task_id);
        self.client_sender
            .send(ToGatewayMessage::TaskUpdate(TaskUpdate {
                id: task_id,
                state: TaskState::Finished,
            }))
            .unwrap();
    }

    fn send_client_task_started(
        &mut self,
        task_id: TaskId,
        worker_id: WorkerId,
        context: SerializedTaskContext,
    ) {
        log::debug!("Informing client about running task={}", task_id);
        self.client_sender
            .send(ToGatewayMessage::TaskUpdate(TaskUpdate {
                id: task_id,
                state: TaskState::Running { worker_id, context },
            }))
            .unwrap();
    }

    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) {
        self.client_sender
            .send(ToGatewayMessage::TaskFailed({
                TaskFailedMessage {
                    id: task_id,
                    info: error_info,
                    cancelled_tasks: consumers_id,
                }
            }))
            .unwrap();
    }

    fn send_client_worker_new(&mut self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        assert!(self
            .client_sender
            .send(ToGatewayMessage::NewWorker(NewWorkerMessage {
                worker_id,
                configuration: configuration.clone(),
            }))
            .is_ok());
    }

    fn send_client_worker_lost(
        &mut self,
        worker_id: WorkerId,
        running_tasks: Vec<TaskId>,
        reason: LostWorkerReason,
    ) {
        if let Err(e) = self
            .client_sender
            .send(ToGatewayMessage::LostWorker(LostWorkerMessage {
                worker_id,
                running_tasks,
                reason,
            }))
        {
            log::error!("Error while sending worker lost message to client: {:?}", e);
        }
    }
}
