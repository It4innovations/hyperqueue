use std::time::Duration;

use crate::internal::common::resources::request::{ResourceRequest, ResourceRequestEntry};
use crate::internal::common::resources::{ResourceId, ResourceRequestVariants};
use crate::resources::{AllocationRequest, NumOfNodes, ResourceAmount};
pub use ResourceRequestBuilder as ResBuilder;

#[derive(Default, Clone)]
pub struct ResourceRequestBuilder {
    n_nodes: NumOfNodes,
    resources: Vec<ResourceRequestEntry>,
    min_time: Duration,
}

impl ResourceRequestBuilder {
    pub fn add<Id: Into<ResourceId>>(self, id: Id, amount: ResourceAmount) -> Self {
        self.add_compact(id, amount)
    }

    pub fn n_nodes(mut self, n_nodes: NumOfNodes) -> Self {
        self.n_nodes = n_nodes;
        self
    }

    pub fn cpus(self, count: ResourceAmount) -> Self {
        self.add(0, count)
    }

    fn _add(&mut self, id: ResourceId, request: AllocationRequest) {
        self.resources.push(ResourceRequestEntry {
            resource_id: id.into(),
            request,
        });
    }

    pub fn add_compact<Id: Into<ResourceId>>(mut self, id: Id, amount: ResourceAmount) -> Self {
        self._add(id.into(), AllocationRequest::Compact(amount));
        self
    }

    pub fn add_force_compact<Id: Into<ResourceId>>(
        mut self,
        id: Id,
        amount: ResourceAmount,
    ) -> Self {
        self._add(id.into(), AllocationRequest::ForceCompact(amount));
        self
    }

    pub fn add_scatter<Id: Into<ResourceId>>(mut self, id: Id, amount: ResourceAmount) -> Self {
        self._add(id.into(), AllocationRequest::Scatter(amount));
        self
    }

    pub fn add_all<Id: Into<ResourceId>>(mut self, id: Id) -> Self {
        self._add(id.into(), AllocationRequest::All);
        self
    }

    pub fn min_time_secs(mut self, secs: u64) -> ResBuilder {
        self.min_time = Duration::new(secs, 0);
        self
    }

    pub fn finish(mut self) -> ResourceRequest {
        // Add 1 cpu if no cpu exists
        if !self.resources.iter().any(|r| r.resource_id == 0.into()) {
            self.resources.insert(
                0,
                ResourceRequestEntry {
                    resource_id: 0.into(),
                    request: AllocationRequest::Compact(1),
                },
            )
        }
        ResourceRequest::new(self.n_nodes, self.min_time, self.resources.into())
    }

    pub fn finish_v(mut self) -> ResourceRequestVariants {
        ResourceRequestVariants::new_simple(self.finish())
    }
}

pub fn cpus_compact(count: ResourceAmount) -> ResBuilder {
    ResBuilder::default().add(0, count)
}
/*
pub fn cpus_force_compact(count: NumOfCpus) -> ResBuilder {
    ResBuilder::default().cpus(CpuRequest::ForceCompact(count))
}
pub fn cpus_scatter(count: NumOfCpus) -> ResBuilder {
    ResBuilder::default().cpus(CpuRequest::Scatter(count))
}
pub fn cpus_all() -> ResBuilder {
    ResBuilder::default().cpus(CpuRequest::All)
}
*/
