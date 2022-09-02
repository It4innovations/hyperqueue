use crate::internal::common::resources::{ResourceAmount, ResourceIndex};
use crate::internal::common::utils::format_comma_delimited;
use crate::internal::common::Set;
use serde::{Deserialize, Serialize};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DescriptorError {
    #[error("Items in a list-based generic resource have to be unique")]
    ResourceListItemsNotUnique,
}

// Do now construct these directly, use the appropriate constructors
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResourceDescriptorKind {
    List {
        values: Vec<ResourceIndex>,
    },
    Groups {
        groups: Vec<Vec<ResourceIndex>>,
    },
    Range {
        start: ResourceIndex,
        // end is inclusive
        end: ResourceIndex,
    },
    Sum {
        size: ResourceAmount,
    },
}

impl ResourceDescriptorKind {
    pub fn regular_sockets(n_sockets: ResourceAmount, socket_size: ResourceAmount) -> Self {
        assert!(n_sockets > 0);
        assert!(socket_size > 0);
        if n_sockets == 1 {
            Self::simple_indices(socket_size as u32)
        } else {
            let mut sockets = Vec::with_capacity(n_sockets as usize);
            let mut i = 0;
            for _ in 0..n_sockets {
                let mut socket = Vec::with_capacity(socket_size as usize);
                for _ in 0..socket_size {
                    socket.push(ResourceIndex::new(i));
                    i += 1;
                }
                sockets.push(socket)
            }
            Self::groups(sockets).unwrap()
        }
    }

    pub fn details(&self) -> String {
        match self {
            ResourceDescriptorKind::List { values } => {
                format!("[{}]", format_comma_delimited(values))
            }
            ResourceDescriptorKind::Groups { groups } => {
                format!(
                    "[{}]",
                    format_comma_delimited(
                        groups
                            .iter()
                            .map(|g| format!("[{}]", format_comma_delimited(g)))
                    )
                )
            }
            ResourceDescriptorKind::Range { start, end } if start == end => format!("[{}]", start),
            ResourceDescriptorKind::Range { start, end } => format!("range({}-{})", start, end),
            ResourceDescriptorKind::Sum { size } => format!("sum({})", size),
        }
    }

    pub fn has_indices(&self) -> bool {
        match self {
            ResourceDescriptorKind::List { .. }
            | ResourceDescriptorKind::Groups { .. }
            | ResourceDescriptorKind::Range { .. } => true,
            ResourceDescriptorKind::Sum { .. } => false,
        }
    }

    fn normalize_indices(
        mut values: Vec<ResourceIndex>,
    ) -> Result<Vec<ResourceIndex>, DescriptorError> {
        let count = values.len();
        values.sort_unstable();
        values.dedup();

        if values.len() < count {
            Err(DescriptorError::ResourceListItemsNotUnique)
        } else {
            Ok(values)
        }
    }

    pub fn groups(mut groups: Vec<Vec<ResourceIndex>>) -> Result<Self, DescriptorError> {
        if groups.len() == 1 {
            Self::list(groups.pop().unwrap())
        } else {
            Ok(ResourceDescriptorKind::Groups {
                groups: groups
                    .into_iter()
                    .map(Self::normalize_indices)
                    .collect::<Result<_, _>>()?,
            })
        }
    }

    pub fn list(values: Vec<ResourceIndex>) -> Result<Self, DescriptorError> {
        Ok(ResourceDescriptorKind::List {
            values: Self::normalize_indices(values)?,
        })
    }

    pub fn simple_indices(size: u32) -> Self {
        assert!(size > 0);
        ResourceDescriptorKind::Range {
            start: ResourceIndex::from(0),
            end: ResourceIndex::from(size - 1),
        }
    }

    pub fn n_groups(&self) -> usize {
        match self {
            ResourceDescriptorKind::List { .. }
            | ResourceDescriptorKind::Range { .. }
            | ResourceDescriptorKind::Sum { .. } => 1,
            ResourceDescriptorKind::Groups { groups } => groups.len(),
        }
    }

    pub fn size(&self) -> ResourceAmount {
        match self {
            ResourceDescriptorKind::List { values } => values.len() as ResourceAmount,
            ResourceDescriptorKind::Range { start, end } if end >= start => {
                (end.as_num() + 1 - start.as_num()) as ResourceAmount
            }
            ResourceDescriptorKind::Range { .. } => 0,
            ResourceDescriptorKind::Sum { size } => *size,
            ResourceDescriptorKind::Groups { groups } => {
                groups.iter().map(|x| x.len() as ResourceAmount).sum()
            }
        }
    }

    pub fn as_groups(&self) -> Vec<Vec<ResourceIndex>> {
        match self {
            ResourceDescriptorKind::List { values } => vec![values.clone()],
            ResourceDescriptorKind::Groups { groups } => groups.clone(),
            ResourceDescriptorKind::Range { start, end } => {
                vec![(start.as_num()..=end.as_num())
                    .map(ResourceIndex::new)
                    .collect::<Vec<_>>()]
            }
            ResourceDescriptorKind::Sum { .. } => Vec::new(),
        }
    }

    pub fn validate(&self) -> crate::Result<()> {
        match self {
            ResourceDescriptorKind::List { values } => {
                let set: Set<_> = values.iter().collect();
                if set.len() != values.len() {
                    return Err("Non unique indices".into());
                }
            }
            ResourceDescriptorKind::Groups { groups } => {
                let set: Set<_> = groups.iter().flatten().collect();
                let size = groups.iter().map(|x| x.len()).sum::<usize>();
                if set.len() != size {
                    return Err("Non unique indices".into());
                }
            }
            ResourceDescriptorKind::Range { .. } => {}
            ResourceDescriptorKind::Sum { .. } => {}
        }
        Ok(())
    }
}

impl std::fmt::Display for ResourceDescriptorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List { values } => {
                write!(f, "[{}]", format_comma_delimited(values))
            }
            Self::Range { start, end } => {
                write!(f, "Range({start}-{end})")
            }
            Self::Sum { size } => write!(f, "Sum({size})"),
            Self::Groups { .. } => todo!(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceDescriptorItem {
    pub name: String,
    pub kind: ResourceDescriptorKind,
}

/// Most precise description of request provided by a worker (without time resource)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceDescriptor {
    pub resources: Vec<ResourceDescriptorItem>,
}

impl ResourceDescriptor {
    pub fn new(mut resources: Vec<ResourceDescriptorItem>) -> Self {
        resources.sort_by(|x, y| x.name.cmp(&y.name));

        ResourceDescriptor { resources }
    }

    pub fn validate(&self) -> crate::Result<()> {
        let mut has_cpus = false;
        for (i, item) in self.resources.iter().enumerate() {
            for item2 in &self.resources[i + 1..] {
                if item2.name == item.name {
                    return Err(format!("Resource {} defined twice", item.name).into());
                }
            }
            item.kind
                .validate()
                .map_err(|e| format!("Invalid resource definition for {}: {:?}", item.name, e))?;

            if item.kind.size() == 0 {
                return Err(format!("Resource {} is empty", item.name).into());
            }
            if item.name == "cpus" {
                has_cpus = true
            }
        }
        if !has_cpus {
            return Err("Resource 'cpus' is missing".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::CPU_RESOURCE_NAME;

    impl ResourceDescriptorItem {
        pub fn range(name: &str, start: u32, end: u32) -> Self {
            ResourceDescriptorItem {
                name: name.to_string(),
                kind: ResourceDescriptorKind::Range {
                    start: start.into(),
                    end: end.into(),
                },
            }
        }

        pub fn sum(name: &str, size: u64) -> Self {
            ResourceDescriptorItem {
                name: name.to_string(),
                kind: ResourceDescriptorKind::Sum { size: size.into() },
            }
        }
    }

    impl ResourceDescriptor {
        pub fn simple(n_cpus: ResourceAmount) -> Self {
            Self::sockets(1, n_cpus)
        }

        pub fn sockets(n_sockets: ResourceAmount, n_cpus_per_socket: ResourceAmount) -> Self {
            ResourceDescriptor::new(vec![ResourceDescriptorItem {
                name: CPU_RESOURCE_NAME.to_string(),
                kind: ResourceDescriptorKind::regular_sockets(n_sockets, n_cpus_per_socket),
            }])
        }
    }
}
