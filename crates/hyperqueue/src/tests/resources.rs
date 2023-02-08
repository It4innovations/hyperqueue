use tako::resources::ResourceDescriptorKind;

pub fn res_kind_range(start: u32, end: u32) -> ResourceDescriptorKind {
    ResourceDescriptorKind::Range {
        start: start.into(),
        end: end.into(),
    }
}

pub fn res_kind_list(items: &[&str]) -> ResourceDescriptorKind {
    ResourceDescriptorKind::List {
        values: items.into_iter().map(|&v| v.to_string()).collect(),
    }
}

pub fn res_kind_groups(groups: &[Vec<&str>]) -> ResourceDescriptorKind {
    ResourceDescriptorKind::Groups {
        groups: groups
            .into_iter()
            .map(|v| v.into_iter().map(|s| s.to_string()).collect())
            .collect(),
    }
}

pub fn res_kind_sum(size: u64) -> ResourceDescriptorKind {
    ResourceDescriptorKind::Sum { size }
}
