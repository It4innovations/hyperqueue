use crate::internal::common::index::IndexVec;
use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::resources::{
    CpuRequest, GenericResourceAmount, NumOfCpus, ResourceDescriptor, ResourceRequest, ResourceVec,
};

// WorkerResources are transformed information from ResourceDescriptor
// but transformed for scheduler needs
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct WorkerResources {
    n_cpus: NumOfCpus,
    n_generic_resources: ResourceVec<GenericResourceAmount>,
}

impl WorkerResources {
    // #[cfg(test)]
    // pub(crate) fn simple(n_cpus: NumOfCpus) -> Self {
    //     WorkerResources::from_description(&ResourceDescriptor::simple(n_cpus), Default::default())
    // }

    pub(crate) fn from_description(
        resource_desc: &ResourceDescriptor,
        resource_map: ResourceMap,
    ) -> Self {
        // We only take maximum needed resource id
        // We are doing it for normalization purposes. It is useful later
        // for WorkerLoad structure that hashed
        let n_generic_resources = resource_desc
            .generic
            .iter()
            .map(|x| resource_map.get_index(&x.name).unwrap().as_num() as usize + 1)
            .max()
            .unwrap_or(0);

        let mut n_generic_resources: ResourceVec<GenericResourceAmount> =
            IndexVec::filled(0, n_generic_resources);

        for descriptor in &resource_desc.generic {
            let position = resource_map.get_index(&descriptor.name).unwrap();
            n_generic_resources[position] = descriptor.kind.size()
        }

        WorkerResources {
            n_cpus: resource_desc
                .cpus
                .iter()
                .map(|socket| socket.len())
                .sum::<usize>() as NumOfCpus,
            n_generic_resources,
        }
    }

    pub(crate) fn is_capable_to_run(&self, request: &ResourceRequest) -> bool {
        let has_cpus = match request.cpus() {
            CpuRequest::Compact(n_cpus)
            | CpuRequest::ForceCompact(n_cpus)
            | CpuRequest::Scatter(n_cpus) => *n_cpus <= self.n_cpus,
            CpuRequest::All => true,
        };
        has_cpus
            && request.generic_requests().iter().all(|r| {
                r.amount
                    <= *self
                        .n_generic_resources
                        .get(r.resource.as_num() as usize)
                        .unwrap_or(&0)
            })
    }

    pub(crate) fn n_cpus(&self, request: &ResourceRequest) -> NumOfCpus {
        match request.cpus() {
            CpuRequest::Compact(n_cpus)
            | CpuRequest::ForceCompact(n_cpus)
            | CpuRequest::Scatter(n_cpus) => *n_cpus,
            CpuRequest::All => self.n_cpus,
        }
    }
}

// This represents a current worker load from server perspective
// Note: It ignores time request, as "remaining time" is "always changing" resource
// while this structure is also used in hashset for parking resources
// It is solved in scheduler by directly calling worker.has_time_to_run

#[derive(Debug, Eq, PartialEq)]
pub struct WorkerLoad {
    n_cpus: NumOfCpus,
    n_generic_resources: ResourceVec<GenericResourceAmount>,
}

impl WorkerLoad {
    pub(crate) fn new(worker_resources: &WorkerResources) -> WorkerLoad {
        WorkerLoad {
            n_cpus: 0,
            n_generic_resources: IndexVec::filled(0, worker_resources.n_generic_resources.len()),
        }
    }

    #[inline]
    pub(crate) fn add_request(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        self.n_cpus += wr.n_cpus(rq);
        for r in rq.generic_requests() {
            self.n_generic_resources[r.resource] += r.amount;
        }
    }

    #[inline]
    pub(crate) fn remove_request(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        self.n_cpus -= wr.n_cpus(rq);
        for r in rq.generic_requests() {
            self.n_generic_resources[r.resource] -= r.amount;
        }
    }

    pub(crate) fn is_underloaded(&self, wr: &WorkerResources) -> bool {
        self.n_cpus < wr.n_cpus
            && self
                .n_generic_resources
                .iter()
                .zip(wr.n_generic_resources.iter())
                .all(|(v, w)| v < w)
    }

    pub(crate) fn is_overloaded(&self, wr: &WorkerResources) -> bool {
        self.n_cpus > wr.n_cpus
            || self
                .n_generic_resources
                .iter()
                .zip(wr.n_generic_resources.iter())
                .any(|(v, w)| v > w)
    }

    pub(crate) fn have_immediate_resources_for_rq(
        &self,
        request: &ResourceRequest,
        wr: &WorkerResources,
    ) -> bool {
        wr.n_cpus(request) + self.n_cpus <= wr.n_cpus
            && request.generic_requests().iter().all(|r| {
                r.amount
                    <= *self
                        .n_generic_resources
                        .get(r.resource.as_num() as usize)
                        .unwrap_or(&0)
            })
    }

    pub(crate) fn have_immediate_resources_for_lb(
        &self,
        lower_bound: &ResourceRequestLowerBound,
        wr: &WorkerResources,
    ) -> bool {
        if lower_bound.empty {
            return false;
        }
        (lower_bound
            .n_cpus
            .map(|n| self.n_cpus + n <= wr.n_cpus)
            .unwrap_or(false)
            || (lower_bound.all && self.n_cpus == 0))
            && (lower_bound
                .n_generic_resources
                .iter()
                .enumerate()
                .all(|(i, lb)| {
                    *self.n_generic_resources.get(i).unwrap_or(&0) + lb
                        <= *wr.n_generic_resources.get(i).unwrap_or(&0)
                }))
    }

    pub(crate) fn is_more_loaded_then(&self, rs: &WorkerLoad) -> bool {
        self.n_cpus > rs.n_cpus
        /* TODO: Extend it also to GenericResources
           Also we are now counting with more or less same workers
           so we should probably compare fraction of resources then resource itself
        */
    }

    #[cfg(test)] // Now used only in tests, feel free to make it public
    pub(crate) fn get_n_cpus(&self) -> NumOfCpus {
        self.n_cpus
    }
}

/// This structure tracks an infimum over a set of task requests
/// requests are added by method "include" to this set.
#[derive(Debug)]
pub struct ResourceRequestLowerBound {
    empty: bool,
    n_cpus: Option<NumOfCpus>,
    all: bool,
    n_generic_resources: ResourceVec<GenericResourceAmount>,
}

impl ResourceRequestLowerBound {
    pub(crate) fn new(n_resource: usize) -> Self {
        ResourceRequestLowerBound {
            empty: true,
            n_cpus: None,
            all: false,
            n_generic_resources: IndexVec::filled(0, n_resource),
        }
    }

    #[cfg(test)]
    pub(crate) fn simple_cpus(n_cpus: Option<NumOfCpus>, all: bool) -> Self {
        ResourceRequestLowerBound {
            empty: false,
            n_cpus,
            all,
            n_generic_resources: Default::default(),
        }
    }

    pub(crate) fn include(&mut self, request: &ResourceRequest) {
        match request.cpus() {
            CpuRequest::Compact(n_cpus)
            | CpuRequest::ForceCompact(n_cpus)
            | CpuRequest::Scatter(n_cpus) => {
                self.n_cpus = self.n_cpus.map(|n| n.min(*n_cpus)).or(Some(*n_cpus));
            }
            CpuRequest::All => {
                self.all = true;
            }
        }
        if self.empty {
            for gr in request.generic_requests() {
                self.n_generic_resources[gr.resource] = gr.amount;
            }
            self.empty = false;
        } else {
            for (i, n) in self.n_generic_resources.iter_mut().enumerate() {
                if *n == 0 {
                    continue;
                }
                if let Some(gr) = request
                    .generic_requests()
                    .iter()
                    .find(|gr| gr.resource.as_num() as usize == i)
                {
                    *n = (*n).min(gr.amount);
                } else {
                    *n = 0;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::ResourceRequest;
    use crate::internal::server::workerload::{
        ResourceRequestLowerBound, WorkerLoad, WorkerResources,
    };
    use crate::internal::tests::utils::resources::{cpus_all, cpus_compact};

    #[test]
    fn worker_lb_include1() {
        let mut lb = ResourceRequestLowerBound::new(0);
        lb.include(&cpus_compact(4).finish());
        assert_eq!(lb.n_cpus, Some(4));
        assert!(!lb.all);

        lb.include(&cpus_compact(2).finish());
        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);

        lb.include(&cpus_compact(4).finish());
        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);

        lb.include(&cpus_all().finish());
        assert_eq!(lb.n_cpus, Some(2));
        assert!(lb.all);

        lb.include(&cpus_compact(1).finish());
        assert_eq!(lb.n_cpus, Some(1));
        assert!(lb.all);
    }

    #[test]
    fn worker_lb_include2() {
        let mut lb = ResourceRequestLowerBound::new(0);
        lb.include(&cpus_all().finish());
        assert_eq!(lb.n_cpus, None);
        assert!(lb.all);
    }

    #[test]
    fn worker_lb_include_generic_resources1() {
        let mut lb = ResourceRequestLowerBound::new(3);
        let rq: ResourceRequest = cpus_compact(2).add_generic(1, 10).finish();
        lb.include(&rq);

        assert_eq!(lb.n_generic_resources, vec![0, 10, 0].into());

        let rbuilder = cpus_compact(3).add_generic(1, 5);
        lb.include(&rbuilder.clone().finish());

        assert_eq!(lb.n_generic_resources, vec![0, 5, 0].into());

        let rbuilder = rbuilder.add_generic(2, 100);
        lb.include(&rbuilder.finish());

        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);
        assert_eq!(lb.n_generic_resources, vec![0, 5, 0].into());
    }

    #[test]
    fn worker_lb_include_generic_resources2() {
        let mut lb = ResourceRequestLowerBound::new(3);
        let rq: ResourceRequest = cpus_compact(2)
            .add_generic(0, 10)
            .add_generic(1, 20)
            .finish();
        lb.include(&rq);

        assert_eq!(lb.n_generic_resources, vec![10, 20, 0].into());

        let rq: ResourceRequest = cpus_compact(3).add_generic(1, 25).finish();
        lb.include(&rq);

        assert_eq!(lb.n_generic_resources, vec![0, 20, 0].into());
        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);
    }

    #[test]
    fn worker_load_check_lb1() {
        let wr = WorkerResources {
            n_cpus: 4,
            n_generic_resources: Default::default(),
        };
        let load = WorkerLoad {
            n_cpus: 2,
            n_generic_resources: Default::default(),
        };

        let lb = ResourceRequestLowerBound::simple_cpus(Some(2), false);
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(Some(3), false);
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(Some(2), true);
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(Some(3), true);
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(None, false);
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(None, true);
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));
    }

    #[test]
    fn worker_load_check_lb2() {
        let wr = WorkerResources {
            n_cpus: 2,
            n_generic_resources: Default::default(),
        };
        let load = WorkerLoad::new(&wr);

        let lb = ResourceRequestLowerBound::simple_cpus(Some(2), false);
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(Some(3), false);
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(Some(2), true);
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(Some(3), true);
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(None, false);
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound::simple_cpus(None, true);
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));
    }

    #[test]
    fn worker_load_check_lb_generic_resources() {
        let wr = WorkerResources {
            n_cpus: 2,
            n_generic_resources: vec![10, 100, 5].into(),
        };
        let load = WorkerLoad::new(&wr);
        let load2 = WorkerLoad {
            n_cpus: 0,
            n_generic_resources: vec![9, 0, 0, 0, 0].into(),
        };

        let lb = ResourceRequestLowerBound {
            empty: false,
            n_cpus: Some(2),
            all: false,
            n_generic_resources: vec![2, 100, 0, 0].into(),
        };
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));
        assert!(!load2.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            empty: false,
            n_cpus: Some(2),
            all: false,
            n_generic_resources: vec![0, 0, 0, 1].into(),
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            empty: false,
            n_cpus: Some(2),
            all: false,
            n_generic_resources: vec![2, 101, 0, 0].into(),
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            empty: false,
            n_cpus: Some(2),
            all: false,
            n_generic_resources: vec![0, 200, 0].into(),
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));
    }
}
