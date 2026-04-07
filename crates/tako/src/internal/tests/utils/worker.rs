use crate::WorkerId;
use crate::internal::server::worker::Worker;
use crate::internal::worker::configuration::OverviewConfiguration;
use crate::resources::{ResourceDescriptor, ResourceDescriptorItem, ResourceIdMap};
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use std::time::{Duration, Instant};

pub struct WorkerBuilder {
    descriptor: ResourceDescriptor,
    time_limit: Option<Duration>,
    group: Option<String>,
    min_utilization: f32,
}

impl WorkerBuilder {
    pub fn empty() -> Self {
        WorkerBuilder {
            descriptor: ResourceDescriptor::new(Default::default(), Default::default()),
            time_limit: None,
            group: None,
            min_utilization: 0.0,
        }
    }

    pub fn new(cpus: u32) -> Self {
        WorkerBuilder {
            descriptor: ResourceDescriptor::simple_cpus(cpus),
            time_limit: None,
            group: None,
            min_utilization: 0.0,
        }
    }

    pub fn min_utilization(mut self, value: f32) -> Self {
        self.min_utilization = value;
        self
    }

    pub fn time_limit(mut self, duration: Duration) -> Self {
        self.time_limit = Some(duration);
        self
    }

    pub fn time_limit_s(self, duration: u64) -> Self {
        self.time_limit(Duration::from_secs(duration))
    }

    pub fn group(mut self, group: &str) -> Self {
        self.group = Some(group.to_string());
        self
    }

    pub fn res_sum(mut self, name: &str, amount: u32) -> Self {
        self.descriptor
            .resources
            .push(ResourceDescriptorItem::sum(name, amount));
        self
    }

    pub fn res_range(mut self, name: &str, start: u32, end: u32) -> Self {
        self.descriptor
            .resources
            .push(ResourceDescriptorItem::range(name, start, end));
        self
    }

    pub fn build_config(&self, worker_id: WorkerId) -> WorkerConfiguration {
        WorkerConfiguration {
            resources: self.descriptor.clone(),
            listen_address: format!("1.1.1.{worker_id}:123"),
            hostname: format!("test{worker_id}"),
            group: self.group.as_deref().unwrap_or("default").to_string(),
            work_dir: Default::default(),
            heartbeat_interval: Duration::from_millis(1000),
            overview_configuration: OverviewConfiguration {
                send_interval: Some(Duration::from_millis(1000)),
                gpu_families: Default::default(),
            },
            idle_timeout: None,
            time_limit: self.time_limit,
            retract_check_interval: Duration::from_secs(30),
            on_server_lost: ServerLostPolicy::Stop,
            min_utilization: self.min_utilization,
            extra: Default::default(),
        }
    }

    pub fn build(&self, worker_id: WorkerId, resource_map: &ResourceIdMap, now: Instant) -> Worker {
        let config = self.build_config(worker_id);
        Worker::new(worker_id, config, resource_map, now)
    }
}
