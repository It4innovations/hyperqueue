use crate::internal::common::resources::GenericResourceId;
use crate::internal::common::Map;

#[derive(Default, Debug)]
pub(crate) struct ResourceIdAllocator {
    resource_names: Map<String, GenericResourceId>,
}

impl ResourceIdAllocator {
    pub fn get_or_allocate_id(&mut self, name: &str) -> GenericResourceId {
        match self.resource_names.get(name) {
            Some(&id) => id,
            None => {
                let id = GenericResourceId::new(self.resource_names.len() as u32);
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
    pub fn get_index(&self, name: &str) -> Option<GenericResourceId> {
        self.resource_names
            .iter()
            .position(|n| n == name)
            .map(|id| GenericResourceId::new(id as u32))
    }

    #[inline]
    pub fn get_name(&self, index: GenericResourceId) -> Option<&str> {
        self.resource_names
            .get(index.as_num() as usize)
            .map(|s| s.as_str())
    }
}
