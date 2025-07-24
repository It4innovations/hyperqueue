use crate::internal::common::Set;
use crate::internal::common::resources::{
    ResourceAmount, ResourceIndex, ResourceLabel, ResourceUnits,
};
use crate::internal::common::utils::has_unique_elements;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

use crate::resources::CPU_RESOURCE_NAME;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DescriptorError {
    #[error("Items in a list-based generic resource have to be unique")]
    ResourceListItemsNotUnique,
    #[error("There has to be at least a single grouop")]
    EmptyGroups,
}

// Do now construct these directly, use the appropriate constructors
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum ResourceDescriptorKind {
    List {
        values: Vec<ResourceLabel>,
    },
    Groups {
        groups: Vec<Vec<ResourceLabel>>,
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
    pub fn regular_sockets(n_sockets: ResourceUnits, socket_size: ResourceUnits) -> Self {
        assert!(n_sockets > 0);
        assert!(socket_size > 0);
        if n_sockets == 1 {
            Self::simple_indices(socket_size)
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
            Self::groups_numeric(sockets).unwrap()
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

    fn normalize_resource_list(
        values: Vec<ResourceLabel>,
    ) -> Result<Vec<ResourceLabel>, DescriptorError> {
        if !has_unique_elements(&values) {
            Err(DescriptorError::ResourceListItemsNotUnique)
        } else {
            Ok(values)
        }
    }

    pub fn groups_numeric(groups: Vec<Vec<ResourceIndex>>) -> Result<Self, DescriptorError> {
        Self::groups(
            groups
                .into_iter()
                .map(|indices| indices.into_iter().map(|i| i.to_string()).collect())
                .collect(),
        )
    }

    pub fn groups(mut groups: Vec<Vec<ResourceLabel>>) -> Result<Self, DescriptorError> {
        match groups.pop() {
            Some(group) => {
                if groups.is_empty() {
                    Self::list(group)
                } else {
                    groups.push(group);
                    Ok(ResourceDescriptorKind::Groups {
                        groups: groups
                            .into_iter()
                            .map(Self::normalize_resource_list)
                            .collect::<Result<_, _>>()?,
                    })
                }
            }
            None => Err(DescriptorError::EmptyGroups),
        }
    }

    pub fn list(values: Vec<ResourceLabel>) -> Result<Self, DescriptorError> {
        Ok(ResourceDescriptorKind::List {
            values: Self::normalize_resource_list(values)?,
        })
    }

    pub fn simple_indices(size: ResourceUnits) -> Self {
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
            ResourceDescriptorKind::List { values } => {
                ResourceAmount::new_units(values.len() as ResourceUnits)
            }
            ResourceDescriptorKind::Range { start, end } if end >= start => {
                ResourceAmount::new_units(end.as_num() + 1 - start.as_num())
            }
            ResourceDescriptorKind::Range { .. } => ResourceAmount::ZERO,
            ResourceDescriptorKind::Sum { size } => *size,
            ResourceDescriptorKind::Groups { groups } => {
                ResourceAmount::new_units(groups.iter().map(|x| x.len() as ResourceUnits).sum())
            }
        }
    }

    pub fn as_groups(&self) -> Vec<Vec<ResourceLabel>> {
        match self {
            ResourceDescriptorKind::List { values } => vec![values.clone()],
            ResourceDescriptorKind::Groups { groups } => groups.clone(),
            ResourceDescriptorKind::Range { start, end } => {
                vec![
                    (start.as_num()..=end.as_num())
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>(),
                ]
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

impl Debug for ResourceDescriptorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceDescriptorKind::List { values } => {
                write!(f, "list({})", values.join(", "))
            }
            ResourceDescriptorKind::Groups { groups } => {
                write!(f, "groups(")?;
                for (index, group) in groups.iter().enumerate() {
                    write!(f, "[{}]", group.join(", "))?;
                    if index < groups.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
            ResourceDescriptorKind::Range { start, end } => {
                write!(f, "range({start}-{end})")
            }
            ResourceDescriptorKind::Sum { size } => {
                write!(f, "sum({size})")
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResourceDescriptorItem {
    pub name: String,
    pub kind: ResourceDescriptorKind,
}

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

    pub fn sum(name: &str, size: u32) -> Self {
        ResourceDescriptorItem {
            name: name.to_string(),
            kind: ResourceDescriptorKind::Sum {
                size: ResourceAmount::new_units(size),
            },
        }
    }
}

impl Debug for ResourceDescriptorItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={:?}", self.name, self.kind)
    }
}

/// Define names of coupled resources
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceDescriptorCoupling {
    pub names: Vec<String>,
}

/// Most precise description of request provided by a worker (without time resource)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResourceDescriptor {
    pub resources: Vec<ResourceDescriptorItem>,
    pub coupling: Option<ResourceDescriptorCoupling>,
}

impl ResourceDescriptor {
    pub fn new(
        mut resources: Vec<ResourceDescriptorItem>,
        coupling: Option<ResourceDescriptorCoupling>,
    ) -> Self {
        resources.sort_by(|x, y| x.name.cmp(&y.name));

        ResourceDescriptor {
            resources,
            coupling,
        }
    }

    pub fn simple_cpus(n_cpus: ResourceUnits) -> Self {
        Self::sockets(1, n_cpus)
    }

    pub fn sockets(n_sockets: ResourceUnits, n_cpus_per_socket: ResourceUnits) -> Self {
        ResourceDescriptor::new(
            vec![ResourceDescriptorItem {
                name: CPU_RESOURCE_NAME.to_string(),
                kind: ResourceDescriptorKind::regular_sockets(n_sockets, n_cpus_per_socket),
            }],
            None,
        )
    }

    pub fn validate(&self, needs_cpus: bool) -> crate::Result<()> {
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

            if item.kind.size().is_zero() {
                return Err(format!("Resource {} is empty", item.name).into());
            }
            if item.name == "cpus" {
                has_cpus = true
            }
        }
        if !has_cpus && needs_cpus {
            return Err("Resource 'cpus' is missing".into());
        }
        if let Some(coupling) = &self.coupling {
            if coupling.names.len() < 2 {
                return Err("Invalid number of coupled resources".into());
            }
            let mut group_size = None;
            for name in &coupling.names {
                if let Some(r) = self.resources.iter().find(|r| &r.name == name) {
                    if let Some(g) = &group_size {
                        if *g != r.kind.n_groups() {
                            return Err(
                                "Coupled resources needs to have the same number of groups".into(),
                            );
                        }
                    } else {
                        group_size = Some(r.kind.n_groups())
                    }
                } else {
                    return Err(format!("Coupling of unknown resource: '{name}'").into());
                }
            }
        }
        Ok(())
    }
}
