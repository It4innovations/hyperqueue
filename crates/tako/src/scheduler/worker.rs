use std::collections::HashSet;

use crate::scheduler::protocol::{WorkerInfo};
use crate::scheduler::task::TaskRef;
use crate::WorkerId;

pub type HostnameId = u64;

#[derive(Debug)]
pub struct Worker {
    pub id: WorkerId,
    pub ncpus: u32,
    pub hostname_id: HostnameId,
    pub tasks: HashSet<TaskRef>,
}

impl Worker {
    pub fn sanity_check(&self, worker_ref: &WorkerRef) {
        for tr in &self.tasks {
            let task = tr.get();
            assert!(task.is_assigned());
            let wr = task.assigned_worker.as_ref().unwrap();
            assert!(wr.eq(worker_ref));
        }
    }

    #[inline]
    pub fn is_underloaded(&self) -> bool {
        let len = self.tasks.len() as u32;
        len < self.ncpus
    }

    #[inline]
    pub fn is_overloaded(&self) -> bool {
        let len = self.tasks.len() as u32;
        len > self.ncpus
    }
}

pub type WorkerRef = crate::common::WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(wi: WorkerInfo, hostname_id: HostnameId) -> Self {
        Self::wrap(Worker {
            id: wi.id,
            ncpus: wi.n_cpus,
            hostname_id,
            tasks: Default::default(),
        })
    }
}
