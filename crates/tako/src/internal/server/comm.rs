use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;

use crate::gateway::{
    LostWorkerMessage, LostWorkerReason, NewWorkerMessage, TaskFailedMessage, TaskState,
    TaskUpdate, ToGatewayMessage,
};
use crate::internal::common::{Map, WrappedRcRefCell};
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{ToWorkerMessage, WorkerOverview};
use crate::internal::server::core::Core;
use crate::internal::transfer::auth::serialize;
use crate::internal::worker::configuration::WorkerConfiguration;
use crate::task::SerializedTaskContext;
use crate::{InstanceId, TaskId, WorkerId};

pub trait Comm {
    fn send_worker_message(&mut self, worker_id: WorkerId, message: &ToWorkerMessage);
    fn broadcast_worker_message(&mut self, message: &ToWorkerMessage);
    fn ask_for_scheduling(&mut self);

    fn send_client_task_finished(&mut self, task_id: TaskId);
    fn send_client_task_started(
        &mut self,
        task_id: TaskId,
        instance_id: InstanceId,
        worker_ids: &[WorkerId],
        context: SerializedTaskContext,
    );
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
    fn send_client_worker_overview(&mut self, overview: Box<WorkerOverview>);
}

type SchedulingCallback = Box<dyn FnOnce(&mut Core)>;

pub struct CommSender {
    workers: Map<WorkerId, UnboundedSender<Bytes>>,
    need_scheduling: bool,
    scheduler_wakeup: Rc<Notify>,
    client_sender: UnboundedSender<ToGatewayMessage>,
    after_scheduling_callbacks: Vec<SchedulingCallback>,
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
            after_scheduling_callbacks: Vec::new(),
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
            panic!(
                "Worker lost while server is running in testing mode with flag '--panic-on-worker-lost'"
            );
        }
        assert!(self.workers.remove(&worker_id).is_some());
    }

    pub fn reset_scheduling_flag(&mut self) {
        self.need_scheduling = false
    }

    pub fn get_scheduling_flag(&self) -> bool {
        self.need_scheduling
    }

    pub fn add_after_scheduling_callback(&mut self, callback: SchedulingCallback) {
        self.after_scheduling_callbacks.push(callback)
    }

    pub fn call_after_scheduling_callbacks(&mut self, core: &mut Core) {
        if !self.after_scheduling_callbacks.is_empty() {
            log::debug!("Running after scheduling callbacks");
            self.after_scheduling_callbacks
                .drain(..)
                .for_each(|x| x(core))
        }
    }
}

impl Comm for CommSender {
    fn send_worker_message(&mut self, worker_id: WorkerId, message: &ToWorkerMessage) {
        let data = serialize(&message).unwrap();
        self.workers
            .get(&worker_id)
            .unwrap()
            .send(data.into())
            .expect("Send to worker failed");
    }

    fn broadcast_worker_message(&mut self, message: &ToWorkerMessage) {
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
        if let Err(error) = self
            .client_sender
            .send(ToGatewayMessage::TaskUpdate(TaskUpdate {
                id: task_id,
                state: TaskState::Finished,
            }))
        {
            log::error!("Error while task finished message to client: {error:?}");
        }
    }

    fn send_client_task_started(
        &mut self,
        task_id: TaskId,
        instance_id: InstanceId,
        worker_ids: &[WorkerId],
        context: SerializedTaskContext,
    ) {
        log::debug!("Informing client about running task={}", task_id);
        if let Err(error) = self
            .client_sender
            .send(ToGatewayMessage::TaskUpdate(TaskUpdate {
                id: task_id,
                state: TaskState::Running {
                    instance_id,
                    worker_ids: worker_ids.into(),
                    context,
                },
            }))
        {
            log::error!("Error while task started message to client: {error:?}");
        }
    }

    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) {
        if let Err(error) = self.client_sender.send(ToGatewayMessage::TaskFailed({
            TaskFailedMessage {
                id: task_id,
                info: error_info,
                cancelled_tasks: consumers_id,
            }
        })) {
            log::error!("Error while task error message to client: {error:?}");
        }
    }

    fn send_client_worker_new(&mut self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        if let Err(error) = self
            .client_sender
            .send(ToGatewayMessage::NewWorker(NewWorkerMessage {
                worker_id,
                configuration: configuration.clone(),
            }))
        {
            log::error!("Error while new worker message to client: {error:?}");
        }
    }

    fn send_client_worker_lost(
        &mut self,
        worker_id: WorkerId,
        running_tasks: Vec<TaskId>,
        reason: LostWorkerReason,
    ) {
        if let Err(error) =
            self.client_sender
                .send(ToGatewayMessage::LostWorker(LostWorkerMessage {
                    worker_id,
                    running_tasks,
                    reason,
                }))
        {
            log::error!("Error while sending worker lost message to client: {error:?}");
        }
    }

    fn send_client_worker_overview(&mut self, overview: Box<WorkerOverview>) {
        if let Err(error) = self
            .client_sender
            .send(ToGatewayMessage::WorkerOverview(overview))
        {
            log::error!("Error while sending worker overview message to client: {error:?}");
        }
    }
}
