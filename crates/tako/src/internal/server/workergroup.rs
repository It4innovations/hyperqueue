use crate::{Set, WorkerId};

pub struct WorkerGroup {
    pub worker_ids: Set<WorkerId>,
}

impl WorkerGroup {
    pub fn new_worker(&mut self, worker_id: WorkerId) {
        assert!(self.worker_ids.insert(worker_id));
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) {
        assert!(self.worker_ids.remove(&worker_id));
    }
}
