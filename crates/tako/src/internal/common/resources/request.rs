use serde::{Deserialize, Serialize};
use std::fmt;

use crate::internal::common::error::DsError;
use crate::internal::common::resources::{NumOfNodes, ResourceAmount, ResourceId};

use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::resources::allocator::ResourceAllocator;
use smallvec::SmallVec;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub enum AllocationRequest {
    Compact(ResourceAmount),
    ForceCompact(ResourceAmount),
    Scatter(ResourceAmount),
    All,
}

impl AllocationRequest {
    pub fn validate(&self) -> crate::Result<()> {
        match &self {
            AllocationRequest::Scatter(n_cpus)
            | AllocationRequest::ForceCompact(n_cpus)
            | AllocationRequest::Compact(n_cpus) => {
                if *n_cpus == 0 {
                    Err(DsError::GenericError(
                        "Zero resources cannot be requested".to_string(),
                    ))
                } else {
                    Ok(())
                }
            }
            AllocationRequest::All => Ok(()),
        }
    }

    pub fn min_amount(&self) -> ResourceAmount {
        match self {
            AllocationRequest::Compact(amount)
            | AllocationRequest::ForceCompact(amount)
            | AllocationRequest::Scatter(amount) => *amount,
            AllocationRequest::All => 1,
        }
    }

    pub fn amount(&self, all: ResourceAmount) -> ResourceAmount {
        match self {
            AllocationRequest::Compact(amount)
            | AllocationRequest::ForceCompact(amount)
            | AllocationRequest::Scatter(amount) => *amount,
            AllocationRequest::All => all,
        }
    }
}

impl fmt::Display for AllocationRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AllocationRequest::Compact(amount) => write!(f, "{amount} compact"),
            AllocationRequest::ForceCompact(amount) => write!(f, "{amount} compact!"),
            AllocationRequest::Scatter(amount) => write!(f, "{amount} scatter"),
            AllocationRequest::All => write!(f, "all"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct ResourceRequestEntry {
    pub resource_id: ResourceId,
    pub request: AllocationRequest,
}

pub type ResourceRequestEntries = SmallVec<[ResourceRequestEntry; 3]>;
pub type TimeRequest = Duration;

#[derive(Default, Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct ResourceRequest {
    n_nodes: NumOfNodes,

    resources: ResourceRequestEntries,

    /// Minimal remaining time of the worker life time needed to START the task
    /// !!! Do not confuse with time_limit.
    /// If task is started and task is running, it is not stopped if
    /// it consumes more. If you need this, see time_limit in task configuration
    /// On worker with not defined life time, this resource is always satisfied.
    #[serde(default)]
    min_time: TimeRequest,
}

impl ResourceRequest {
    pub fn new(
        n_nodes: NumOfNodes,
        time: TimeRequest,
        mut resources: ResourceRequestEntries,
    ) -> ResourceRequest {
        resources.sort_unstable_by_key(|r| r.resource_id);
        ResourceRequest {
            n_nodes,
            resources,
            min_time: time,
        }
    }

    pub fn is_multi_node(&self) -> bool {
        self.n_nodes > 0
    }

    pub fn n_nodes(&self) -> NumOfNodes {
        self.n_nodes
    }

    pub fn min_time(&self) -> TimeRequest {
        self.min_time
    }

    pub fn entries(&self) -> &ResourceRequestEntries {
        &self.resources
    }

    pub fn sort_key(&self, ac: &ResourceAllocator) -> (f32, TimeRequest) {
        let score = self.entries().iter().map(|e| ac.difficulty_score(e)).sum();
        (score, self.min_time)
    }

    pub fn validate(&self) -> crate::Result<()> {
        if self.resources.is_empty() && self.n_nodes == 0 {
            return Err("Resource request is empty".into());
        }
        for entry in &self.resources {
            entry.request.validate()?;
        }
        for pair in self.resources.windows(2) {
            if pair[0].resource_id >= pair[1].resource_id {
                return Err("Request are not sorted or unique".into());
            }
        }
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceRequestVariants {
    variants: SmallVec<[ResourceRequest; 1]>,
}

impl ResourceRequestVariants {
    pub fn new(variants: SmallVec<[ResourceRequest; 1]>) -> Self {
        ResourceRequestVariants { variants }
    }

    pub fn sort_key(&self, allocator: &ResourceAllocator) -> (f32, TimeRequest) {
        /*
          The following unwrap is ok since there has to be always at least at least one
          runnable configuration. Otherwise this task should not be assigned to the worker.
          If the unwrap fails, then it means a fatal error in server scheduler.
        */
        let (i, score) = self
            .variants
            .iter()
            .enumerate()
            .find_map(|(i, rq)| {
                if allocator.is_capable_to_run(rq) {
                    Some((i, rq.sort_key(allocator)))
                } else {
                    None
                }
            })
            .unwrap();
        self.variants[i + 1..]
            .iter()
            .fold(score, |(score, time), rq| {
                let (score2, time2) = rq.sort_key(allocator);
                (score.min(score2), time.min(time2))
            })
    }

    pub fn find_index(&self, rq: &ResourceRequest) -> Option<usize> {
        if self.variants.len() == 1 {
            Some(0)
        } else {
            self.variants.iter().position(|r| r == rq)
        }
    }

    pub fn is_trivial(&self) -> bool {
        self.variants.len() == 1
    }

    pub fn trivial_request(&self) -> Option<&ResourceRequest> {
        if self.variants.len() == 1 {
            Some(&self.variants[0])
        } else {
            None
        }
    }

    // Temporary code for migration, eventually this should be removed from the code base
    pub fn unwrap_first(&self) -> &ResourceRequest {
        assert_eq!(self.variants.len(), 1);
        &self.variants[0]
    }

    pub fn requests(&self) -> &[ResourceRequest] {
        &self.variants
    }

    pub fn validate(&self) -> crate::Result<()> {
        if self.variants.is_empty() {
            return Err("Resource are empty".into());
        }
        let is_multi_node = self.variants[0].is_multi_node();
        for rq in &self.variants {
            rq.validate()?;
            if is_multi_node != rq.is_multi_node() {
                return Err("Resources mixes multi-node and non-multi-node requests".into());
            }
        }
        Ok(())
    }

    pub fn is_multi_node(&self) -> bool {
        self.variants[0].is_multi_node()
    }

    pub fn filter_runnable(&self, resources: &WorkerResources) -> Self {
        let variants: SmallVec<[ResourceRequest; 1]> = self
            .variants
            .iter()
            .filter(|rq| resources.is_capable_to_run_request(rq))
            .cloned()
            .collect();
        assert!(!variants.is_empty()); // Valid variants are non empty
        ResourceRequestVariants { variants }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::ResourceRequestVariants;
    use crate::internal::tests::utils::resources::ResBuilder;
    use crate::resources::ResourceRequest;
    use smallvec::smallvec;
    impl ResourceRequestVariants {
        pub fn new_simple(rq: ResourceRequest) -> ResourceRequestVariants {
            ResourceRequestVariants::new(smallvec![rq])
        }
    }

    #[test]
    fn test_resource_request_validate() {
        let rq = ResBuilder::default()
            .add_all(0)
            .add(10, 4)
            .add(7, 6)
            .add(10, 6)
            .finish();
        assert!(rq.validate().is_err())
    }
}
