use std::ops::{Deref, DerefMut};

use crate::internal::common::Map;
use crate::internal::server::worker::Worker;
use crate::WorkerId;

#[derive(Default, Debug)]
pub struct WorkerMap {
    workers: Map<WorkerId, Worker>,
}

impl WorkerMap {
    #[inline]
    pub fn get_worker(&self, worker_id: WorkerId) -> &Worker {
        &self.workers[&worker_id]
    }

    #[inline]
    pub fn get_worker_mut(&mut self, worker_id: WorkerId) -> &mut Worker {
        self.workers.get_mut(&worker_id).expect("Worker not found")
    }

    #[inline]
    pub fn get_workers_mut(&mut self) -> impl Iterator<Item = &mut Worker> {
        self.workers.values_mut()
    }
}

impl Deref for WorkerMap {
    type Target = Map<WorkerId, Worker>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.workers
    }
}
impl DerefMut for WorkerMap {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.workers
    }
}
