use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;

use crate::WorkerId;
use crate::events::EventProcessor;
use crate::internal::common::{Map, WrappedRcRefCell};
use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::transfer::auth::serialize;

pub trait Comm {
    fn send_worker_message(&mut self, worker_id: WorkerId, message: &ToWorkerMessage);
    fn broadcast_worker_message(&mut self, message: &ToWorkerMessage);
    fn ask_for_scheduling(&mut self);

    fn client(&mut self) -> &mut dyn EventProcessor;
}

pub struct CommSender {
    workers: Map<WorkerId, UnboundedSender<Bytes>>,
    need_scheduling: bool,
    scheduler_wakeup: Rc<Notify>,
    client_events: Option<Box<dyn EventProcessor>>,
    panic_on_worker_lost: bool,
}

pub type CommSenderRef = WrappedRcRefCell<CommSender>;

impl CommSenderRef {
    pub fn new(scheduler_wakeup: Rc<Notify>, panic_on_worker_lost: bool) -> Self {
        WrappedRcRefCell::wrap(CommSender {
            workers: Default::default(),
            scheduler_wakeup,
            client_events: None,
            need_scheduling: false,
            panic_on_worker_lost,
        })
    }

    pub fn set_client_events(&self, client_events: Box<dyn EventProcessor>) {
        self.get_mut().client_events = Some(client_events);
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
        if self.workers.is_empty() {
            return;
        }
        let data: Bytes = serialize(&message).unwrap().into();
        for (worker_id, sender) in &self.workers {
            if sender.send(data.clone()).is_err() {
                log::error!("Failed to send message to worker: {worker_id}");
            }
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
    fn client(&mut self) -> &mut dyn EventProcessor {
        self.client_events.as_mut().unwrap().as_mut()
    }
}
