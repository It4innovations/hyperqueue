use crate::gateway::ResourceRequestVariants as ClientResourceRequestVariants;
use crate::internal::common::Map;
use crate::internal::common::resources::{ResourceId, ResourceRqId};
use crate::internal::server::core::Core;
use crate::resources::{ResourceAllocRequest, ResourceRequest, ResourceRequestVariants};
use serde::{Deserialize, Serialize};

pub const CPU_RESOURCE_ID: ResourceId = ResourceId(0);

pub const CPU_RESOURCE_NAME: &str = "cpus";
pub const NVIDIA_GPU_RESOURCE_NAME: &str = "gpus/nvidia";
pub const AMD_GPU_RESOURCE_NAME: &str = "gpus/amd";
pub const MEM_RESOURCE_NAME: &str = "mem";

#[derive(Debug)]
pub(crate) struct GlobalResourceMapping {
    resource_rq_from_id: ResourceRqMap,
    resource_rq_to_id: Map<ResourceRequestVariants, ResourceRqId>,
    resource_names: Map<String, ResourceId>,
}

impl Default for GlobalResourceMapping {
    fn default() -> Self {
        let mut resource_names = Map::new();
        /* Fix id for cpus */
        resource_names.insert(CPU_RESOURCE_NAME.to_string(), CPU_RESOURCE_ID);
        GlobalResourceMapping {
            resource_rq_from_id: Default::default(),
            resource_names,
            resource_rq_to_id: Map::new(),
        }
    }
}

impl GlobalResourceMapping {
    pub(crate) fn convert_client_resource_rq(
        &mut self,
        resources: &ClientResourceRequestVariants,
    ) -> ResourceRequestVariants {
        ResourceRequestVariants::new(
            resources
                .variants
                .iter()
                .map(|rq| {
                    ResourceRequest::new(
                        rq.n_nodes,
                        rq.min_time,
                        rq.resources
                            .iter()
                            .map(|r| {
                                let resource_id = self.get_or_create_resource_id(&r.resource);
                                ResourceAllocRequest {
                                    resource_id,
                                    request: r.policy.clone(),
                                }
                            })
                            .collect(),
                    )
                })
                .collect(),
        )
    }

    pub fn get_or_create_resource_id(&mut self, name: &str) -> ResourceId {
        match self.resource_names.get(name) {
            Some(&id) => id,
            None => {
                let id = ResourceId::new(self.resource_names.len() as u32);
                log::debug!("New generic resource registered '{name}' as {id}");
                self.resource_names.insert(name.to_string(), id);
                id
            }
        }
    }

    /// Create an immutable snapshot of resource name map.
    #[inline]
    pub fn create_resource_id_map(&self) -> ResourceIdMap {
        let mut resource_names: Vec<_> = self.resource_names.keys().cloned().collect();
        resource_names.sort_unstable_by_key(|name| *self.resource_names.get(name).unwrap());

        ResourceIdMap { resource_names }
    }

    #[inline]
    pub fn get_resource_rq_map(&self) -> &ResourceRqMap {
        &self.resource_rq_from_id
    }

    #[cfg(test)]
    pub fn get_resource_rq_id(&mut self, rqv: &ResourceRequestVariants) -> ResourceRqId {
        *self.resource_rq_to_id.get(rqv).unwrap()
    }

    pub fn get_or_create_resource_rq_id(
        &mut self,
        rqv: &ResourceRequestVariants,
    ) -> (ResourceRqId, bool) {
        match self.resource_rq_to_id.get(rqv) {
            Some(&id) => (id, false),
            None => {
                let mut id = ResourceRqId::new(
                    self.resource_rq_to_id.len() as u32 * 2
                        + if rqv.is_multi_node() { 1 } else { 0 },
                );
                log::debug!("New resource request registered {rqv:?} as {id}");
                self.resource_rq_to_id.insert(rqv.clone(), id);
                self.resource_rq_from_id.insert(id, rqv.clone());
                (id, true)
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct ResourceIdMap {
    resource_names: Vec<String>,
}

impl ResourceIdMap {
    #[inline]
    pub fn from_vec(resource_names: Vec<String>) -> Self {
        Self { resource_names }
    }

    #[cfg(test)]
    pub fn from_ref(resource_names: &[&str]) -> Self {
        Self {
            resource_names: resource_names.iter().map(|x| x.to_string()).collect(),
        }
    }

    #[inline]
    pub fn into_vec(self) -> Vec<String> {
        self.resource_names
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.resource_names.len()
    }

    #[inline]
    pub fn get_index(&self, name: &str) -> Option<ResourceId> {
        self.resource_names
            .iter()
            .position(|n| n == name)
            .map(|id| ResourceId::new(id as u32))
    }

    #[inline]
    pub fn get_name(&self, index: ResourceId) -> Option<&str> {
        self.resource_names
            .get(index.as_num() as usize)
            .map(|s| s.as_str())
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceRqMap(Map<ResourceRqId, ResourceRequestVariants>);

impl ResourceRqMap {
    pub fn insert(&mut self, rq_id: ResourceRqId, rqv: ResourceRequestVariants) {
        assert!(self.0.insert(rq_id, rqv).is_none());
    }

    #[inline]
    pub fn get(&self, rq_id: &ResourceRqId) -> &ResourceRequestVariants {
        self.0.get(rq_id).unwrap()
    }

    #[cfg(test)]
    pub fn get_or_create(&mut self, rqv: ResourceRequestVariants) -> ResourceRqId {
        if let Some(rq_id) = self
            .0
            .iter()
            .find_map(|(rq_id, rqv2)| (&rqv == rqv2).then(|| *rq_id))
        {
            rq_id
        } else {
            let mut new_id = ResourceRqId::new(
                self.0.len() as u32 * 2 + if rqv.is_multi_node() { 1 } else { 0 },
            );
            self.0.insert(new_id, rqv);
            new_id
        }
    }
}
