use crate::internal::common::index::IndexVec;
use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::resources::request::ResourceAllocRequest;
use crate::internal::common::resources::{
    ResourceAmount, ResourceDescriptor, ResourceId, ResourceRequest, ResourceRequestVariants,
    ResourceVec,
};
use crate::internal::messages::worker::WorkerResourceCounts;
use crate::{Map, Set, TaskId};
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
            .get(resource_id)
            .copied()
            .unwrap_or(ResourceAmount::ZERO)
    }

    pub(crate) fn get_or_none(&self, resource_id: ResourceId) -> Option<ResourceAmount> {
        self.n_resources.get(resource_id).copied()
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

        let mut n_resources: ResourceVec<ResourceAmount> =
            IndexVec::filled(ResourceAmount::ZERO, resource_count);

        for descriptor in &resource_desc.resources {
            let position = resource_map.get_index(&descriptor.name).unwrap();
            n_resources[position] = descriptor.kind.size()
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

    pub(crate) fn is_lowerbound_for_request(&self, request: &ResourceRequest) -> bool {
        request.entries().iter().all(|r| {
            if let Some(has) = self.get_or_none(r.resource_id) {
                let ask = r.request.min_amount();
                ask <= has
            } else {
                true
            }
        })
    }

    pub(crate) fn is_capable_to_run(&self, rqv: &ResourceRequestVariants) -> bool {
        rqv.requests()
            .iter()
            .any(|rq| self.is_capable_to_run_request(rq))
    }

    pub(crate) fn is_lowerbound_for(
        &self,
        rqv: &ResourceRequestVariants,
        filter_fn: impl Fn(&ResourceRequest) -> bool,
    ) -> bool {
        rqv.requests()
            .iter()
            .any(|rq| filter_fn(rq) && self.is_lowerbound_for_request(rq))
    }

    pub(crate) fn is_capable_to_run_with(
        &self,
        rqv: &ResourceRequestVariants,
        filter_fn: impl Fn(&ResourceRequest) -> bool,
    ) -> bool {
        rqv.requests()
            .iter()
            .any(|rq| filter_fn(rq) && self.is_capable_to_run_request(rq))
    }

    pub(crate) fn to_transport(&self) -> WorkerResourceCounts {
        WorkerResourceCounts {
            n_resources: self.n_resources.deref().clone(),
        }
    }

    pub(crate) fn max_amount(&self, entry: &ResourceAllocRequest) -> ResourceAmount {
        entry.request.amount(self.get(entry.resource_id))
    }

    pub fn difficulty_score(&self, request: &ResourceRequest) -> u64 {
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

    pub fn difficulty_score_of_rqv(&self, rqv: &ResourceRequestVariants) -> u64 {
        rqv.requests()
            .iter()
            .map(|r| self.difficulty_score(r))
            .min()
            .unwrap_or(0)
    }
}

// This represents a current worker load from server perspective
// Note: It ignores time request, as "remaining time" is "always changing" resource
// while this structure is also used in hashset for parking resources
// It is solved in scheduler by directly calling worker.has_time_to_run

#[derive(Debug, Eq, PartialEq)]
pub struct WorkerLoad {
    n_resources: ResourceVec<ResourceAmount>,

    /// The map stores task_ids of requests for which non-first resource alternative is used
    /// i.e., if all tasks has only 1 option in resource requests, this map will be empty
    non_first_rq: Map<TaskId, usize>,
    round_robin_counter: usize,
}

impl WorkerLoad {
    pub(crate) fn new(worker_resources: &WorkerResources) -> WorkerLoad {
        WorkerLoad {
            n_resources: IndexVec::filled(ResourceAmount::ZERO, worker_resources.n_resources.len()),
            non_first_rq: Default::default(),
            round_robin_counter: 0,
        }
    }

    fn _add(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        for r in rq.entries() {
            self.n_resources[r.resource_id] += r.request.amount(wr.n_resources[r.resource_id]);
        }
    }

    pub(crate) fn add_request(
        &mut self,
        task_id: TaskId,
        rqv: &ResourceRequestVariants,
        wr: &WorkerResources,
    ) {
        if let Some(rq) = rqv.trivial_request() {
            self._add(rq, wr);
            return;
        }
        let idx: usize = rqv
            .requests()
            .iter()
            .enumerate()
            .find_map(|(i, rq)| {
                if self.have_immediate_resources_for_rq(rq, wr) {
                    Some(i)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                let v = self.round_robin_counter.wrapping_add(1);
                self.round_robin_counter = v;
                v % rqv.requests().len()
            });
        self._add(&rqv.requests()[idx], wr);
        if idx != 0 {
            self.non_first_rq.insert(task_id, idx);
        }
    }

    pub(crate) fn remove_request(
        &mut self,
        task_id: TaskId,
        rqv: &ResourceRequestVariants,
        wr: &WorkerResources,
    ) {
        let idx = self.non_first_rq.remove(&task_id).unwrap_or(0);
        for r in rqv.requests()[idx].entries() {
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
            .get(resource_id)
            .copied()
            .unwrap_or(ResourceAmount::ZERO)
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

    pub(crate) fn have_immediate_resources_for_rqv(
        &self,
        requests: &ResourceRequestVariants,
        wr: &WorkerResources,
    ) -> bool {
        requests
            .requests()
            .iter()
            .any(|r| self.have_immediate_resources_for_rq(r, wr))
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

    pub(crate) fn load_wrt_rqv(&self, wr: &WorkerResources, rqv: &ResourceRequestVariants) -> u32 {
        rqv.requests()
            .iter()
            .map(|r| self.load_wrt_request(wr, r))
            .min()
            .unwrap_or(0)
    }

    pub(crate) fn load_wrt_request(&self, wr: &WorkerResources, request: &ResourceRequest) -> u32 {
        let mut result = 0;
        for entry in request.entries() {
            let count = wr
                .n_resources
                .get(entry.resource_id)
                .copied()
                .unwrap_or(ResourceAmount::ZERO);
            if count.is_zero() {
                return 0;
            }
            let load = self
                .n_resources
                .get(entry.resource_id)
                .copied()
                .unwrap_or(ResourceAmount::ZERO);
            result += (load.total_fractions() * 512 / count.total_fractions()) as u32;
        }
        result
    }

    pub(crate) fn utilization(&self, wr: &WorkerResources) -> f32 {
        let mut utilization = 0.0f32;
        for (n, w) in self.n_resources.iter().zip(wr.n_resources.iter()) {
            utilization = utilization.max(if w.is_zero() {
                0.0
            } else {
                n.as_f32() / w.as_f32()
            });
        }
        utilization
    }
}

/// This structure tracks an infimum over a set of task requests
///
/// Requests are added by method "include" to this set.
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

    pub(crate) fn include_rqv(&mut self, rqv: &ResourceRequestVariants) {
        for rq in rqv.requests() {
            self.include(rq)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::ResourceRequestVariants;
    use crate::internal::server::workerload::{
        ResourceRequestLowerBound, WorkerLoad, WorkerResources,
    };
    use crate::internal::tests::utils::resources::{cpus_compact, ra_builder, ResBuilder};
    use crate::resources::{ResourceAmount, ResourceUnits};
    use crate::TaskId;
    use smallvec::smallvec;

    pub fn wr_builder(units: &[ResourceUnits]) -> WorkerResources {
        WorkerResources {
            n_resources: ra_builder(units),
        }
    }

    #[test]
    fn worker_load_check_lb() {
        let wr = wr_builder(&[2, 10, 100, 5]);
        let load = WorkerLoad::new(&wr);
        let load2 = WorkerLoad {
            n_resources: ra_builder(&[0, 9, 0, 0, 0, 0]),
            non_first_rq: Default::default(),
            round_robin_counter: 0,
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

    #[test]
    fn worker_load_check_lb_with_variants() {
        let mut lb = ResourceRequestLowerBound::new();
        let rq1 = ResBuilder::default().add(0, 2).add(1, 2).finish();
        let rq2 = ResBuilder::default().add(0, 4).finish();

        let rqv = ResourceRequestVariants::new(smallvec![rq1, rq2]);
        lb.include_rqv(&rqv);

        assert_eq!(lb.request_set.len(), 2);
    }

    #[test]
    fn worker_load_variants() {
        let wr = wr_builder(&[13, 4, 5]);
        let mut load = WorkerLoad::new(&wr);
        let rq1 = ResBuilder::default().add(0, 2).add(1, 2).finish();
        let rq2 = ResBuilder::default().add(0, 4).finish();
        let rqv = ResourceRequestVariants::new(smallvec![rq1, rq2]);

        load.add_request(TaskId::new_test(1), &rqv, &wr);
        assert_eq!(load.n_resources, ra_builder(&[2, 2, 0]));
        assert!(load.non_first_rq.is_empty());

        load.add_request(TaskId::new_test(2), &rqv, &wr);
        assert_eq!(load.n_resources, ra_builder(&[4, 4, 0]));
        assert!(load.non_first_rq.is_empty());

        load.add_request(TaskId::new_test(3), &rqv, &wr);
        assert_eq!(load.n_resources, ra_builder(&[8, 4, 0]));
        assert_eq!(load.non_first_rq.len(), 1);
        assert_eq!(load.non_first_rq.get(&TaskId::new_test(3)), Some(&1));

        load.add_request(TaskId::new_test(4), &rqv, &wr);
        assert_eq!(load.n_resources, ra_builder(&[12, 4, 0]));
        assert_eq!(load.non_first_rq.len(), 2);
        assert_eq!(load.non_first_rq.get(&TaskId::new_test(4)), Some(&1));

        load.add_request(TaskId::new_test(5), &rqv, &wr);
        assert!(
            load.n_resources == ra_builder(&[16, 4, 0])
                || load.n_resources == ra_builder(&[14, 6, 0])
        );
        let resources = load.n_resources.clone();

        load.remove_request(TaskId::new_test(3), &rqv, &wr);
        assert_eq!(
            load.n_resources,
            vec![
                resources[0.into()] - ResourceAmount::new_units(4),
                resources[1.into()],
                ResourceAmount::ZERO,
            ]
            .into()
        );
        assert!(load.non_first_rq.get(&TaskId::new_test(3)).is_none());

        load.remove_request(TaskId::new_test(1), &rqv, &wr);
        assert_eq!(
            load.n_resources,
            vec![
                resources[0.into()] - ResourceAmount::new_units(6),
                resources[1.into()] - ResourceAmount::new_units(2),
                ResourceAmount::ZERO,
            ]
            .into()
        );
        assert!(load.non_first_rq.get(&TaskId::new_test(1)).is_none());
    }
}
