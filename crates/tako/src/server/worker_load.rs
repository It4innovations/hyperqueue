use crate::common::resources::{CpuRequest, NumOfCpus, ResourceDescriptor, ResourceRequest};

// WorkerResources are transformed information from ResourceDescriptor
// but transformed for scheduler needs
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct WorkerResources {
    n_cpus: NumOfCpus,
}

impl WorkerResources {
    #[cfg(test)]
    pub fn simple(n_cpus: NumOfCpus) -> Self {
        WorkerResources::from_description(&ResourceDescriptor::simple(n_cpus))
    }

    pub fn from_description(resource_desc: &ResourceDescriptor) -> Self {
        WorkerResources {
            n_cpus: resource_desc
                .cpus
                .iter()
                .map(|socket| socket.len())
                .sum::<usize>() as NumOfCpus,
        }
    }

    pub fn is_capable_to_run(&self, request: &ResourceRequest) -> bool {
        match request.cpus() {
            CpuRequest::Compact(n_cpus)
            | CpuRequest::ForceCompact(n_cpus)
            | CpuRequest::Scatter(n_cpus) => *n_cpus <= self.n_cpus,
            CpuRequest::All => true,
        }
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
#[derive(Debug, Eq, PartialEq, Default)]
pub struct WorkerLoad {
    n_cpus: NumOfCpus,
}

impl WorkerLoad {
    #[inline]
    pub fn add_request(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        self.n_cpus += wr.n_cpus(rq);
    }

    #[inline]
    pub fn remove_request(&mut self, rq: &ResourceRequest, wr: &WorkerResources) {
        self.n_cpus -= wr.n_cpus(rq);
    }

    pub fn is_underloaded(&self, wr: &WorkerResources) -> bool {
        self.n_cpus < wr.n_cpus
    }

    pub fn is_overloaded(&self, wr: &WorkerResources) -> bool {
        self.n_cpus > wr.n_cpus
    }

    /*pub fn is_overloaded_without_request(
        &self,
        wr: &WorkerResources,
        request: &ResourceRequest,
    ) -> bool {
        self.n_cpus > wr.n_cpus
    }*/

    pub fn have_immediate_resources_for_rq(
        &self,
        request: &ResourceRequest,
        wr: &WorkerResources,
    ) -> bool {
        wr.n_cpus(request) + self.n_cpus <= wr.n_cpus
    }

    pub fn have_immediate_resources_for_lb(
        &self,
        lower_bound: &ResourceRequestLowerBound,
        wr: &WorkerResources,
    ) -> bool {
        lower_bound
            .n_cpus
            .map(|n| self.n_cpus + n <= wr.n_cpus)
            .unwrap_or(false)
            || (lower_bound.all && self.n_cpus == 0)
    }

    pub fn is_more_loaded_then(&self, rs: &WorkerLoad) -> bool {
        self.n_cpus > rs.n_cpus
    }

    pub fn get_n_cpus(&self) -> NumOfCpus {
        self.n_cpus
    }
}

#[derive(Debug, Default)]
pub struct ResourceRequestLowerBound {
    // If Some(x) then
    n_cpus: Option<NumOfCpus>,
    all: bool,
}

impl ResourceRequestLowerBound {
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
    }
}

#[cfg(test)]
mod tests {
    use crate::common::resources::{CpuRequest, ResourceRequest};
    use crate::server::worker_load::{ResourceRequestLowerBound, WorkerLoad, WorkerResources};

    #[test]
    pub fn worker_lb_include1() {
        let mut lb = ResourceRequestLowerBound::default();
        lb.include(&ResourceRequest::new(CpuRequest::Compact(4)));
        assert_eq!(lb.n_cpus, Some(4));
        assert!(!lb.all);

        lb.include(&ResourceRequest::new(CpuRequest::Compact(2)));
        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);

        lb.include(&ResourceRequest::new(CpuRequest::Compact(4)));
        assert_eq!(lb.n_cpus, Some(2));
        assert!(!lb.all);

        lb.include(&ResourceRequest::new(CpuRequest::All));
        assert_eq!(lb.n_cpus, Some(2));
        assert!(lb.all);

        lb.include(&ResourceRequest::new(CpuRequest::Compact(1)));
        assert_eq!(lb.n_cpus, Some(1));
        assert!(lb.all);
    }

    #[test]
    pub fn worker_lb_include2() {
        let mut lb = ResourceRequestLowerBound::default();
        lb.include(&ResourceRequest::new(CpuRequest::All));
        assert_eq!(lb.n_cpus, None);
        assert!(lb.all);
    }

    #[test]
    pub fn worker_load_check_lb1() {
        let load = WorkerLoad { n_cpus: 2 };
        let wr = WorkerResources { n_cpus: 4 };

        let lb = ResourceRequestLowerBound {
            n_cpus: Some(2),
            all: false,
        };
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: Some(3),
            all: false,
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: Some(2),
            all: true,
        };
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: Some(3),
            all: true,
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: None,
            all: false,
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: None,
            all: true,
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));
    }

    #[test]
    pub fn worker_load_check_lb2() {
        let load = WorkerLoad { n_cpus: 0 };
        let wr = WorkerResources { n_cpus: 2 };

        let lb = ResourceRequestLowerBound {
            n_cpus: Some(2),
            all: false,
        };
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: Some(3),
            all: false,
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: Some(2),
            all: true,
        };
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: Some(3),
            all: true,
        };
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: None,
            all: false,
        };
        assert!(!load.have_immediate_resources_for_lb(&lb, &wr));

        let lb = ResourceRequestLowerBound {
            n_cpus: None,
            all: true,
        };
        assert!(load.have_immediate_resources_for_lb(&lb, &wr));
    }
}
