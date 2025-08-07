use crate::Map;
use crate::internal::common::index::IndexVec;
use crate::internal::common::resources::ResourceId;
use crate::resources::{
    ResourceDescriptor, ResourceDescriptorKind, ResourceIndex, ResourceLabel, ResourceMap,
};
use std::borrow::Cow;

/// Maps resource indices to string labels.
#[cfg_attr(test, derive(Default))]
pub struct ResourceLabelMap {
    resources: IndexVec<ResourceId, Map<ResourceIndex, ResourceLabel>>,
}

impl ResourceLabelMap {
    pub fn new(descriptor: &ResourceDescriptor, map: &ResourceMap) -> Self {
        let mut resources: IndexVec<ResourceId, _> = vec![Default::default(); map.len()].into();

        for resource in &descriptor.resources {
            let index = map.get_index(&resource.name).unwrap();
            match &resource.kind {
                ResourceDescriptorKind::List { values } => {
                    resources[index] = values
                        .clone()
                        .into_iter()
                        .enumerate()
                        .map(|(index, label)| (ResourceIndex::new(index as u32), label))
                        .collect();
                }
                ResourceDescriptorKind::Groups { groups } => {
                    resources[index] = groups
                        .clone()
                        .into_iter()
                        .flatten()
                        .enumerate()
                        .map(|(index, label)| (ResourceIndex::new(index as u32), label))
                        .collect();
                }
                ResourceDescriptorKind::Range { .. } | ResourceDescriptorKind::Sum { .. } => {}
            }
        }
        Self { resources }
    }

    pub fn get_label(&self, resource: ResourceId, index: ResourceIndex) -> Cow<'_, str> {
        self.resources
            .get(resource)
            .and_then(|map| map.get(&index).map(|label| label.into()))
            .unwrap_or_else(|| index.to_string().into())
    }

    pub fn get_index(&self, resource: ResourceId, label: &ResourceLabel) -> Option<ResourceIndex> {
        self.resources
            .get(resource)
            .and_then(|map| map.iter().find(|(_, l)| l == &label))
            .map(|(&index, _)| index)
    }
}
