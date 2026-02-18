use crate::internal::server::workermap::WorkerMap;
use crate::resources::{NumOfNodes, ResourceRequest, ResourceRequestVariants};
use crate::{Set, WorkerId};
use std::time::Instant;

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

    pub fn size(&self) -> NumOfNodes {
        self.worker_ids.len() as NumOfNodes
    }

    pub fn is_empty(&self) -> bool {
        self.worker_ids.is_empty()
    }

    pub fn is_capable_to_run_rq(
        &self,
        rq: &ResourceRequest,
        now: Instant,
        worker_map: &WorkerMap,
    ) -> bool {
        let mut target_nodes = if rq.is_multi_node() { rq.n_nodes() } else { 1 };
        for w_id in &self.worker_ids {
            let worker = worker_map.get_worker(*w_id);
            if worker.is_capable_to_run(rq, now) {
                target_nodes = target_nodes.saturating_sub(1);
                if target_nodes == 0 {
                    return true;
                }
            }
        }
        false
    }

    pub fn is_capable_to_run(
        &self,
        rqv: &ResourceRequestVariants,
        now: Instant,
        worker_map: &WorkerMap,
    ) -> bool {
        rqv.requests()
            .iter()
            .any(|rq| self.is_capable_to_run_rq(rq, now, worker_map))
    }
}
