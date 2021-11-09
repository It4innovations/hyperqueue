use crate::common::resources::{
    CpuRequest, GenericResourceAmount, NumOfCpus, ResourceDescriptor, ResourceRequest,
};

// WorkerResources are transformed information from ResourceDescriptor
// but transformed for scheduler needs
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct WorkerResources {
    n_cpus: NumOfCpus,
    n_generic_resources: Vec<GenericResourceAmount>,
}

impl WorkerResources {
    #[cfg(test)]
    pub fn simple(n_cpus: NumOfCpus) -> Self {
        WorkerResources::from_description(&ResourceDescriptor::simple(n_cpus), &[])
    }

    pub fn from_description(resource_desc: &ResourceDescriptor, resource_names: &[String]) -> Self {
        // We only take maximum needed resource id
        // We are doing it for normalization purposes. It is useful later
        // for WorkerLoad structure that hashed
        let n_generic_resources = resource_desc
            .generic
            .iter()
            .map(|x| resource_names.iter().position(|n| n == &x.name).unwrap() + 1)
            .max()
            .unwrap_or(0);

        let mut n_generic_resources = vec![0; n_generic_resources];

        for descriptor in &resource_desc.generic {
            let position = resource_names
                .iter()
                .position(|n| n == &descriptor.name)
                .unwrap();
            n_generic_resources[position] = descriptor.kind.size()
        }
        resource_desc
            .generic
            .iter()
            .map(|x| resource_names.iter().position(|n| n == &x.name).unwrap())
            .max()
            .unwrap_or(0);

        WorkerResources {
            n_cpus: resource_desc
                .cpus
                .iter()
                .map(|socket| socket.len())
                .sum::<usize>() as NumOfCpus,
            n_generic_resources,
        }
    }

    pub fn is_capable_to_run(&self, request: &ResourceRequest) -> bool {
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
                        .get(r.resource as usize)
                        .unwrap_or(&0)
            })
    }

    pub fn n_cpus(&self, request: &ResourceRequest) -> NumOfCpus {
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
    n_generic_resources: Vec<GenericResourceAmount>,
}

impl WorkerLoad {
    pub fn new(worker_resources: &WorkerResources) -> WorkerLoad {
        WorkerLoad {
            n_cpus: 0,
            n_generic_resources: vec![0; worker_resources.n_generic_resources.len()],
        }
    }

    #[inline]
    pub fn add_request(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        self.n_cpus += wr.n_cpus(rq);
        for (r, v) in self
            .n_generic_resources
            .iter_mut()
            .zip(wr.n_generic_resources.iter())
        {
            *r += v;
        }
    }

    #[inline]
    pub fn remove_request(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        self.n_cpus -= wr.n_cpus(rq);
        for (r, v) in self
            .n_generic_resources
            .iter_mut()
            .zip(wr.n_generic_resources.iter())
        {
            *r -= v;
        }
    }

    pub fn is_underloaded(&self, wr: &WorkerResources) -> bool {
        self.n_cpus < wr.n_cpus
            && self
                .n_generic_resources
                .iter()
                .zip(wr.n_generic_resources.iter())
                .all(|(v, w)| v < w)
    }

    pub fn is_overloaded(&self, wr: &WorkerResources) -> bool {
        self.n_cpus > wr.n_cpus
            || self
                .n_generic_resources
                .iter()
                .zip(wr.n_generic_resources.iter())
                .any(|(v, w)| v > w)
    }

    pub fn have_immediate_resources_for_rq(
        &self,
        request: &ResourceRequest,
        wr: &WorkerResources,
    ) -> bool {
        wr.n_cpus(request) + self.n_cpus <= wr.n_cpus
            && request.generic_requests().iter().all(|r| {
                r.amount
                    <= *self
                        .n_generic_resources
                        .get(r.resource as usize)
                        .unwrap_or(&0)
            })
    }

    pub fn have_immediate_resources_for_lb(
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

    pub fn is_more_loaded_then(&self, rs: &WorkerLoad) -> bool {
        self.n_cpus > rs.n_cpus
        /* TODO: Extend it also to GenericResources
           Also we are now counting with more or less same workers
           so we should probably compare fraction of resources then resource itself
        */
    }

    pub fn get_n_cpus(&self) -> NumOfCpus {
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
    n_generic_resources: Vec<GenericResourceAmount>,
}

impl ResourceRequestLowerBound {
    pub fn new(n_resource: usize) -> Self {
        let mut n_generic_resources = Vec::new();
        n_generic_resources.resize(n_resource, 0);
        ResourceRequestLowerBound {
            empty: true,
            n_cpus: None,
            all: false,
            n_generic_resources,
        }
    }

    #[cfg(test)]
    pub fn simple_cpus(n_cpus: Option<NumOfCpus>, all: bool) -> Self {
        ResourceRequestLowerBound {
            empty: false,
            n_cpus,
            all,
            n_generic_resources: Default::default(),
        }
    }

    pub fn include(&mut self, request: &ResourceRequest) {
        match request.cpus() {
            CpuRequest::Compact(n_cpus)
            | CpuRequest::ForceCompact(n_cpus)
            | CpuRequest::Scatter(n_cpus) => {
                self.n_cpus = self
                    .n_cpus
                    .map(|n| n.min(*n_cpus))
                    .or_else(|| Some(*n_cpus));
            }
            CpuRequest::All => {
                self.all = true;
            }
        }
        if self.empty {
            for gr in request.generic_requests() {
                self.n_generic_resources[gr.resource as usize] = gr.amount;
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
                    .find(|gr| gr.resource as usize == i)
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
    use crate::common::resources::{CpuRequest, GenericResourceRequest, ResourceRequest};
    use crate::server::worker_load::{ResourceRequestLowerBound, WorkerLoad, WorkerResources};

    #[test]
    pub fn worker_lb_include1() {
        let mut lb = ResourceRequestLowerBound::new(0);
        lb.include(&CpuRequest::Compact(4).into());
        assert_eq!(lb.n_cpus, Some(4));
        assert!(!lb.all);

        lb.include(&CpuRequest::Compact(2).into());
        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);

        lb.include(&CpuRequest::Compact(4).into());
        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);

        lb.include(&CpuRequest::All.into());
        assert_eq!(lb.n_cpus, Some(2));
        assert!(lb.all);

        lb.include(&CpuRequest::Compact(1).into());
        assert_eq!(lb.n_cpus, Some(1));
        assert!(lb.all);
    }

    #[test]
    pub fn worker_lb_include2() {
        let mut lb = ResourceRequestLowerBound::new(0);
        lb.include(&CpuRequest::All.into());
        assert_eq!(lb.n_cpus, None);
        assert!(lb.all);
    }

    #[test]
    pub fn worker_lb_include_generic_resources1() {
        let mut lb = ResourceRequestLowerBound::new(3);
        let mut rq: ResourceRequest = CpuRequest::Compact(2).into();
        rq.add_generic_request(GenericResourceRequest {
            resource: 1,
            amount: 10,
        });
        lb.include(&rq);

        assert_eq!(lb.n_generic_resources, vec![0, 10, 0]);

        let mut rq: ResourceRequest = CpuRequest::Compact(3).into();
        rq.add_generic_request(GenericResourceRequest {
            resource: 1,
            amount: 5,
        });
        lb.include(&rq);

        assert_eq!(lb.n_generic_resources, vec![0, 5, 0]);

        rq.add_generic_request(GenericResourceRequest {
            resource: 2,
            amount: 100,
        });
        lb.include(&rq);

        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);
        assert_eq!(lb.n_generic_resources, vec![0, 5, 0]);
    }

    #[test]
    pub fn worker_lb_include_generic_resources2() {
        let mut lb = ResourceRequestLowerBound::new(3);
        let mut rq: ResourceRequest = CpuRequest::Compact(2).into();
        rq.add_generic_request(GenericResourceRequest {
            resource: 0,
            amount: 10,
        });
        rq.add_generic_request(GenericResourceRequest {
            resource: 1,
            amount: 20,
        });
        lb.include(&rq);

        assert_eq!(lb.n_generic_resources, vec![10, 20, 0]);

        let mut rq: ResourceRequest = CpuRequest::Compact(3).into();
        rq.add_generic_request(GenericResourceRequest {
            resource: 1,
            amount: 25,
        });
        lb.include(&rq);

        assert_eq!(lb.n_generic_resources, vec![0, 20, 0]);
        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);
    }

    #[test]
    pub fn worker_load_check_lb1() {
        let wr = WorkerResources {
            n_cpus: 4,
            n_generic_resources: Vec::new(),
        };
        let load = WorkerLoad {
            n_cpus: 2,
            n_generic_resources: Vec::new(),
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
    pub fn worker_load_check_lb2() {
        let wr = WorkerResources {
            n_cpus: 2,
            n_generic_resources: vec![],
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
    pub fn worker_load_check_lb_generic_resources() {
        let wr = WorkerResources {
            n_cpus: 2,
            n_generic_resources: vec![10, 100, 5],
        };
        let load = WorkerLoad::new(&wr);
        let load2 = WorkerLoad {
            n_cpus: 0,
            n_generic_resources: vec![9, 0, 0, 0, 0],
        };

        let lb = ResourceRequestLowerBound {
            empty: false,
            n_cpus: Some(2),
            all: false,
            n_generic_resources: vec![2, 100, 0, 0],
        };
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));
        assert!(!load2.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            empty: false,
            n_cpus: Some(2),
            all: false,
            n_generic_resources: vec![0, 0, 0, 1],
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            empty: false,
            n_cpus: Some(2),
            all: false,
            n_generic_resources: vec![2, 101, 0, 0],
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            empty: false,
            n_cpus: Some(2),
            all: false,
            n_generic_resources: vec![0, 200, 0],
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));
    }
}
