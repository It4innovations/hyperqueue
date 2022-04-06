use crate::common::resources::{
    CpuRequest, GenericResourceAmount, GenericResourceId, GenericResourceRequest, NumOfCpus,
    NumOfNodes, ResourceRequest,
};
use derive_builder::Builder;
use std::time::Duration;

pub use ResourceRequestConfigBuilder as ResBuilder;

#[derive(Builder, Default, Clone)]
#[builder(pattern = "owned", derive(Clone))]
pub struct ResourceRequestConfig {
    #[builder(default)]
    n_nodes: NumOfNodes,
    #[builder(default)]
    cpus: CpuRequest,
    #[builder(default)]
    generic: Vec<GenericResourceRequest>,
    #[builder(default)]
    min_time: Duration,
}

impl ResourceRequestConfigBuilder {
    pub fn add_generic<Id: Into<GenericResourceId>>(
        mut self,
        id: Id,
        amount: GenericResourceAmount,
    ) -> Self {
        self.generic
            .get_or_insert_with(|| vec![])
            .push(GenericResourceRequest {
                resource: id.into(),
                amount,
            });
        self
    }

    pub fn min_time_secs(self, secs: u64) -> ResourceRequestConfigBuilder {
        self.min_time(Duration::new(secs, 0))
    }

    pub fn finish(self) -> ResourceRequest {
        let ResourceRequestConfig {
            n_nodes,
            cpus,
            generic,
            min_time,
        }: ResourceRequestConfig = self.build().unwrap();
        ResourceRequest::new(n_nodes, cpus, min_time, generic.into())
    }
}

pub fn cpus_compact(count: NumOfCpus) -> ResBuilder {
    ResBuilder::default().cpus(CpuRequest::Compact(count))
}
pub fn cpus_force_compact(count: NumOfCpus) -> ResBuilder {
    ResBuilder::default().cpus(CpuRequest::ForceCompact(count))
}
pub fn cpus_scatter(count: NumOfCpus) -> ResBuilder {
    ResBuilder::default().cpus(CpuRequest::Scatter(count))
}
pub fn cpus_all() -> ResBuilder {
    ResBuilder::default().cpus(CpuRequest::All)
}
