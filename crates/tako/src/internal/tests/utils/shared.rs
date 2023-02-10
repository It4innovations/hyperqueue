use crate::internal::worker::resources::allocator::ResourceAllocator;
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::resources::{
    ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind, ResourceMap,
};

pub fn res_kind_range(start: u32, end: u32) -> ResourceDescriptorKind {
    ResourceDescriptorKind::Range {
        start: start.into(),
        end: end.into(),
    }
}

pub fn res_kind_list(items: &[&str]) -> ResourceDescriptorKind {
    ResourceDescriptorKind::List {
        values: items.iter().map(|&v| v.to_string()).collect(),
    }
}

pub fn res_kind_groups(groups: &[Vec<&str>]) -> ResourceDescriptorKind {
    ResourceDescriptorKind::Groups {
        groups: groups
            .iter()
            .map(|v| v.iter().map(|s| s.to_string()).collect())
            .collect(),
    }
}

pub fn res_kind_sum(size: u64) -> ResourceDescriptorKind {
    ResourceDescriptorKind::Sum { size }
}

pub fn res_item(name: &str, kind: ResourceDescriptorKind) -> ResourceDescriptorItem {
    ResourceDescriptorItem {
        name: name.to_string(),
        kind,
    }
}

pub fn res_allocator_from_descriptor(descriptor: ResourceDescriptor) -> ResourceAllocator {
    let mut names = vec![];
    for item in &descriptor.resources {
        names.push(item.name.clone());
    }

    let resource_map = ResourceMap::from_vec(names);
    let label_resource_map = ResourceLabelMap::new(&descriptor, &resource_map);
    let allocator = ResourceAllocator::new(&descriptor, &resource_map, &label_resource_map);
    allocator.validate();
    allocator
}
