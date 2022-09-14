use crate::internal::common::index::IndexVec;
use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::resources::request::ResourceRequestEntry;
use crate::internal::common::resources::{
    ResourceAmount, ResourceDescriptor, ResourceId, ResourceRequest, ResourceVec,
};
use crate::internal::messages::worker::WorkerResourceCounts;
use crate::resources::AllocationRequest;
use crate::Set;
use std::ops::Deref;

// WorkerResources are transformed information from ResourceDescriptor
// but transformed for scheduler needs
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct WorkerResources {
    n_resources: ResourceVec<ResourceAmount>,
}

impl WorkerResources {
    pub(crate) fn from_transport(msg: WorkerResourceCounts) -> Self {
        WorkerResources {
            n_resources: msg.n_resources.into(),
        }
    }

    pub(crate) fn get(&self, resource_id: ResourceId) -> ResourceAmount {
        self.n_resources
            .get(resource_id.as_num() as usize)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn from_description(
        resource_desc: &ResourceDescriptor,
        resource_map: &ResourceMap,
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

        let mut n_resources: ResourceVec<ResourceAmount> = IndexVec::filled(0, resource_count);

        for descriptor in &resource_desc.resources {
            let position = resource_map.get_index(&descriptor.name).unwrap();
            n_resources[position] = descriptor.kind.size()
        }

        WorkerResources { n_resources }
    }

    pub(crate) fn is_capable_to_run(&self, request: &ResourceRequest) -> bool {
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

    pub(crate) fn max_amount(&self, entry: &ResourceRequestEntry) -> ResourceAmount {
        match entry.request {
            AllocationRequest::Compact(amount)
            | AllocationRequest::ForceCompact(amount)
            | AllocationRequest::Scatter(amount) => amount,
            AllocationRequest::All => self.get(entry.resource_id),
        }
    }

    pub fn difficulty_score(&self, request: &ResourceRequest) -> u32 {
        let mut result = 0;
        for entry in request.entries() {
            let count = self
                .n_resources
                .get(entry.resource_id.as_num() as usize)
                .copied()
                .unwrap_or(0);
            if count == 0 {
                return 0;
            }
            result += ((entry.request.amount(count) * 512) / (count * 512)) as u32;
        }
        result
    }
}

// This represents a current worker load from server perspective
// Note: It ignores time request, as "remaining time" is "always changing" resource
// while this structure is also used in hashset for parking resources
// It is solved in scheduler by directly calling worker.has_time_to_run

#[derive(Debug, Eq, PartialEq)]
pub struct WorkerLoad {
    n_resources: ResourceVec<ResourceAmount>,
}

impl WorkerLoad {
    pub(crate) fn new(worker_resources: &WorkerResources) -> WorkerLoad {
        WorkerLoad {
            n_resources: IndexVec::filled(0, worker_resources.n_resources.len()),
        }
    }

    #[inline]
    pub(crate) fn add_request(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        for r in rq.entries() {
            self.n_resources[r.resource_id] += r.request.amount(wr.n_resources[r.resource_id]);
        }
    }

    #[inline]
    pub(crate) fn remove_request(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        for r in rq.entries() {
            self.n_resources[r.resource_id] -= r.request.amount(wr.n_resources[r.resource_id]);
        }
    }

    pub(crate) fn is_underloaded(&self, wr: &WorkerResources) -> bool {
        self.n_resources
            .iter()
            .zip(wr.n_resources.iter())
            .all(|(v, w)| v < w)
    }

    pub(crate) fn is_overloaded(&self, wr: &WorkerResources) -> bool {
        self.n_resources
            .iter()
            .zip(wr.n_resources.iter())
            .any(|(v, w)| v > w)
    }

    pub(crate) fn get(&self, resource_id: ResourceId) -> ResourceAmount {
        self.n_resources
            .get(resource_id.as_num() as usize)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn have_immediate_resources_for_rq(
        &self,
        request: &ResourceRequest,
        wr: &WorkerResources,
    ) -> bool {
        request.entries().iter().all(|r| {
            let amount = wr.max_amount(r);
            amount + self.get(r.resource_id) <= wr.get(r.resource_id)
        })
    }

    pub(crate) fn have_immediate_resources_for_lb(
        &self,
        lower_bound: &ResourceRequestLowerBound,
        wr: &WorkerResources,
    ) -> bool {
        lower_bound
            .request_set
            .iter()
            .any(|r| self.have_immediate_resources_for_rq(r, wr))
    }

    pub(crate) fn load_wrt_request(&self, wr: &WorkerResources, request: &ResourceRequest) -> u32 {
        let mut result = 0;
        for entry in request.entries() {
            let count = wr
                .n_resources
                .get(entry.resource_id.as_num() as usize)
                .copied()
                .unwrap_or(0);
            if count == 0 {
                return 0;
            }
            let load = self
                .n_resources
                .get(entry.resource_id.as_num() as usize)
                .copied()
                .unwrap_or(0);
            result += ((load * 512) / (count * 512)) as u32;
        }
        result
    }
}

/// This structure tracks an infimum over a set of task requests
/// requests are added by method "include" to this set.
#[derive(Debug, Default)]
pub struct ResourceRequestLowerBound {
    request_set: Set<ResourceRequest>,
}

impl ResourceRequestLowerBound {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn include(&mut self, request: &ResourceRequest) {
        // TODO: Technically it would sufficient store only requests that are not covered by existing,
        // TODO: Need to ivestigate how coverage works for multinode
        if !self.request_set.contains(request) {
            self.request_set.insert(request.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::server::workerload::{
        ResourceRequestLowerBound, WorkerLoad, WorkerResources,
    };
    use crate::internal::tests::utils::resources::cpus_compact;

    #[test]
    fn worker_load_check_lb() {
        let wr = WorkerResources {
            n_resources: vec![2, 10, 100, 5].into(),
        };
        let load = WorkerLoad::new(&wr);
        let load2 = WorkerLoad {
            n_resources: vec![0, 9, 0, 0, 0, 0].into(),
        };

        let mut lb = ResourceRequestLowerBound::new();
        lb.include(&cpus_compact(2).add_all(1).finish());
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));
        assert!(!load2.have_immediate_resources_for_lb(&lb, &wr));

        let mut lb = ResourceRequestLowerBound::new();
        lb.include(&cpus_compact(2).add(1, 2).add(2, 100).finish());
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));
        assert!(!load2.have_immediate_resources_for_lb(&lb, &wr));

        let mut lb = ResourceRequestLowerBound::new();
        lb.include(&cpus_compact(2).add(4, 1).finish());
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));
        assert!(!load2.have_immediate_resources_for_lb(&lb, &wr));

        lb.include(&cpus_compact(2).add(2, 101).finish());
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));
        assert!(!load2.have_immediate_resources_for_lb(&lb, &wr));
        lb.include(&cpus_compact(2).finish());
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));
        assert!(load2.have_immediate_resources_for_lb(&lb, &wr));
    }
}
