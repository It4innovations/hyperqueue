use crate::{Set, WorkerId};

pub struct WorkerGroup {
    worker_ids: Set<WorkerId>,
}

impl WorkerGroup {
    pub fn new(worker_ids: Set<WorkerId>) -> Self {
        WorkerGroup { worker_ids }
    }

    pub fn worker_ids(&self) -> impl Iterator<Item = WorkerId> + '_ {
        self.worker_ids.iter().copied()
    }

    pub fn new_worker(&mut self, worker_id: WorkerId) {
        assert!(self.worker_ids.insert(worker_id));
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) {
        assert!(self.worker_ids.remove(&worker_id));
    }

    pub fn size(&self) -> usize {
        self.worker_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.worker_ids.is_empty()
    }
}
