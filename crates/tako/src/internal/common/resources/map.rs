use crate::internal::common::resources::ResourceId;
use crate::internal::common::Map;

pub const CPU_RESOURCE_ID: ResourceId = ResourceId(0);

pub const CPU_RESOURCE_NAME: &str = "cpus";
pub const NVIDIA_GPU_RESOURCE_NAME: &str = "gpus/nvidia";
pub const AMD_GPU_RESOURCE_NAME: &str = "gpus/amd";
pub const MEM_RESOURCE_NAME: &str = "mem";

#[derive(Debug)]
pub(crate) struct ResourceIdAllocator {
    resource_names: Map<String, ResourceId>,
}

impl Default for ResourceIdAllocator {
    fn default() -> Self {
        let mut resource_names = Map::new();
        /* Fix id for cpus */
        resource_names.insert(CPU_RESOURCE_NAME.to_string(), CPU_RESOURCE_ID);
        ResourceIdAllocator { resource_names }
    }
}

impl ResourceIdAllocator {
    pub fn get_or_allocate_id(&mut self, name: &str) -> ResourceId {
        match self.resource_names.get(name) {
            Some(&id) => id,
            None => {
                let id = ResourceId::new(self.resource_names.len() as u32);
                log::debug!("New generic resource registered '{}' as {}", name, id);
                self.resource_names.insert(name.to_string(), id);
                id
            }
        }
    }

    /// Create an immutable snapshot of resource name map.
    #[inline]
    pub fn create_map(&self) -> ResourceMap {
        let mut resource_names: Vec<_> = self.resource_names.keys().cloned().collect();
        resource_names.sort_unstable_by_key(|name| *self.resource_names.get(name).unwrap());

        ResourceMap { resource_names }
    }

    #[inline]
    pub fn resource_count(&self) -> usize {
        self.resource_names.len()
    }
}

#[derive(Default, Debug)]
pub struct ResourceMap {
    resource_names: Vec<String>,
}

impl ResourceMap {
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
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
