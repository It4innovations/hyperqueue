use crate::internal::common::index::IndexVec;
use crate::internal::common::resources::map::ResourceIdMap;
use crate::internal::common::resources::{
    ResourceAmount, ResourceDescriptor, ResourceId, ResourceRequest, ResourceRequestVariants,
    ResourceVec,
};
use crate::internal::messages::worker::WorkerResourceCounts;
use crate::resources::CPU_RESOURCE_ID;
use serde_json::json;
use std::ops::Deref;

const MAX_TASK_PER_WORKER: u64 = 1024;

// WorkerResources are transformed information from ResourceDescriptor
// but transformed for scheduler needs
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct WorkerResources {
    n_resources: ResourceVec<ResourceAmount>,
}

impl WorkerResources {
    pub(crate) fn get(&self, resource_id: ResourceId) -> ResourceAmount {
        self.n_resources
            .get(resource_id)
            .copied()
            .unwrap_or(ResourceAmount::ZERO)
    }

    pub(crate) fn iter_pairs(&self) -> impl Iterator<Item = (ResourceId, ResourceAmount)> {
        self.n_resources
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(idx, c)| {
                if !c.is_zero() {
                    Some((ResourceId::new(idx as u32), c))
                } else {
                    None
                }
            })
    }

    pub(crate) fn iter_amounts(&self) -> impl Iterator<Item = ResourceAmount> {
        self.n_resources.iter().copied()
    }

    pub(crate) fn from_description(
        resource_desc: &ResourceDescriptor,
        resource_map: &ResourceIdMap,
    ) -> Self {
        // We only take maximum needed resource id
        // We are doing it for normalization purposes. It is useful later
        // for WorkerLoad structure that hashed
        let resource_count = resource_desc
            .resources
            .iter()
            .map(|x| resource_map.get_index(&x.name).unwrap().as_num() as usize + 1)
            .max()
            .unwrap_or(0);

        let mut n_resources: ResourceVec<ResourceAmount> =
            IndexVec::filled(ResourceAmount::ZERO, resource_count);

        for descriptor in &resource_desc.resources {
            let position = resource_map.get_index(&descriptor.name).unwrap();
            let size = descriptor.kind.size();
            n_resources[position] = size;
        }

        WorkerResources { n_resources }
    }

    pub(crate) fn is_capable_to_run_request(&self, request: &ResourceRequest) -> bool {
        request.entries().iter().all(|r| {
            let ask = r.request.min_amount();
            let has = self.get(r.resource_id);
            ask <= has
        })
    }

    pub(crate) fn to_transport(&self) -> WorkerResourceCounts {
        WorkerResourceCounts {
            n_resources: self.n_resources.deref().clone(),
        }
    }

    fn compute_difficulty_score(&self, request: &ResourceRequest) -> u64 {
        let mut result = 0;
        for entry in request.entries() {
            let count = self
                .n_resources
                .get(entry.resource_id)
                .copied()
                .unwrap_or(ResourceAmount::ZERO);
            if count.is_zero() {
                return 0;
            }
            result += entry.request.amount(count).total_fractions() / count.total_fractions();
        }
        result
    }

    pub fn compute_difficulty_score_of_rqv(&self, rqv: &ResourceRequestVariants) -> u64 {
        rqv.requests()
            .iter()
            .map(|r| self.compute_difficulty_score(r))
            .min()
            .unwrap_or(0)
    }

    pub fn dump(&self) -> serde_json::Value {
        json!({
            "n_resources": self.n_resources.iter().map(|x| x.total_fractions()).collect::<Vec<_>>(),
        })
    }

    pub fn task_max_count_for_request(&self, request: &ResourceRequest) -> u32 {
        request
            .entries()
            .iter()
            .map(|e| {
                if let Some(requested) = e.request.amount_or_none_if_all() {
                    self.n_resources
                        .get(e.resource_id)
                        .copied()
                        .unwrap_or(ResourceAmount::ZERO)
                        .div(requested)
                        .min(MAX_TASK_PER_WORKER) as u32
                } else if self
                    .n_resources
                    .get(e.resource_id)
                    .is_none_or(|r| r.is_zero())
                {
                    0
                } else {
                    1
                }
            })
            .min()
            .unwrap_or(0)
    }

    pub fn task_max_count(&self, rqv: &ResourceRequestVariants) -> u32 {
        // TODO: More precise computation when we have more than 1 variant
        // TODO: Cache this value in Worker
        rqv.requests()
            .iter()
            .map(|r| self.task_max_count_for_request(r))
            .sum::<u32>()
    }

    pub fn remove(&mut self, rq: &ResourceRequest) {
        for entry in rq.entries() {
            if let Some(amount) = entry.request.amount_or_none_if_all() {
                assert!(self.n_resources[entry.resource_id] >= amount);
                self.n_resources[entry.resource_id] -= amount;
            } else {
                self.n_resources[entry.resource_id] = ResourceAmount::ZERO;
            }
        }
    }

    pub fn remove_multiple(&mut self, rq: &ResourceRequest, n: u32) {
        for entry in rq.entries() {
            if let Some(amount) = entry.request.amount_or_none_if_all() {
                let a = amount.times(n);
                assert!(self.n_resources[entry.resource_id] >= a);
                self.n_resources[entry.resource_id] -= a;
            } else {
                self.n_resources[entry.resource_id] = ResourceAmount::ZERO;
            }
        }
    }

    pub fn remove_multiple_masked(&mut self, rq: &ResourceRequest, n: u32, r_id: ResourceId) {
        for entry in rq.entries() {
            if entry.resource_id == r_id {
                if let Some(amount) = entry.request.amount_or_none_if_all() {
                    let a = amount.times(n);
                    assert!(self.n_resources[entry.resource_id] >= a);
                    self.n_resources[entry.resource_id] -= a;
                } else {
                    self.n_resources[entry.resource_id] = ResourceAmount::ZERO;
                }
                return;
            }
        }
    }

    pub fn add(&mut self, rq: &ResourceRequest, all: &WorkerResources) {
        for entry in rq.entries() {
            if let Some(amount) = entry.request.amount_or_none_if_all() {
                self.n_resources[entry.resource_id] += amount;
            } else {
                self.n_resources[entry.resource_id] = all.get(entry.resource_id);
            }
        }
    }

    pub fn add_multiple(&mut self, rq: &ResourceRequest, all: &WorkerResources, n: u32) {
        for entry in rq.entries() {
            if let Some(amount) = entry.request.amount_or_none_if_all() {
                self.n_resources[entry.resource_id] += amount.times(n);
            } else {
                self.n_resources[entry.resource_id] = all.get(entry.resource_id);
            }
        }
    }

    pub fn utilization(&self, all_resources: &WorkerResources) -> f32 {
        let n = all_resources.n_resources[CPU_RESOURCE_ID];
        if n.is_max() {
            // If resource has maximal value, so it is not a real resource request, but generated by
            // partial descriptor in the scheduler query, so we do not know the actual amount of this resources
            // so we return 1.0 to match any utilization
            1.0
        } else {
            self.n_resources[CPU_RESOURCE_ID].as_f32()
                / all_resources.n_resources[CPU_RESOURCE_ID].as_f32()
        }
    }
}
