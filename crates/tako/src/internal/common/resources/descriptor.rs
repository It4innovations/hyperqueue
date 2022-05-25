use crate::internal::common::resources::{
    CpuId, GenericResourceAmount, GenericResourceIndex, NumOfCpus,
};
use crate::internal::common::utils::format_comma_delimited;
use crate::internal::common::Set;
use serde::{Deserialize, Serialize};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DescriptorError {
    #[error("Items in a list-based generic resource have to be unique")]
    GenericResourceListItemsNotUnique,
}

// Do now construct these directly, use the appropriate constructors
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum GenericResourceDescriptorKind {
    List {
        values: Vec<GenericResourceIndex>,
    },
    Range {
        start: GenericResourceIndex,
        end: GenericResourceIndex,
    },
    // TODO: Named(Vec<String>),
    Sum {
        size: GenericResourceAmount,
    },
}

impl GenericResourceDescriptorKind {
    pub fn list(mut values: Vec<GenericResourceIndex>) -> Result<Self, DescriptorError> {
        let count = values.len();
        values.sort_unstable();
        values.dedup();

        if values.len() < count {
            Err(DescriptorError::GenericResourceListItemsNotUnique)
        } else {
            Ok(GenericResourceDescriptorKind::List { values })
        }
    }

    pub fn size(&self) -> GenericResourceAmount {
        match self {
            GenericResourceDescriptorKind::List { values } => values.len() as GenericResourceAmount,
            GenericResourceDescriptorKind::Range { start, end } if end >= start => {
                (end.as_num() + 1 - start.as_num()) as u64
            }
            GenericResourceDescriptorKind::Range { .. } => 0,
            GenericResourceDescriptorKind::Sum { size } => *size,
        }
    }
}

impl std::fmt::Display for GenericResourceDescriptorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GenericResourceDescriptorKind::List { values } => {
                write!(f, "List({})", format_comma_delimited(values))
            }
            GenericResourceDescriptorKind::Range { start, end } => {
                write!(f, "Range({start}-{end})")
            }
            GenericResourceDescriptorKind::Sum { size } => write!(f, "Sum({size})"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenericResourceDescriptor {
    pub name: String,
    pub kind: GenericResourceDescriptorKind,
}

impl GenericResourceDescriptor {
    pub fn list<Index: Into<GenericResourceIndex>>(
        name: &str,
        values: Vec<Index>,
    ) -> Result<Self, DescriptorError> {
        Ok(GenericResourceDescriptor {
            name: name.to_string(),
            kind: GenericResourceDescriptorKind::list(
                values.into_iter().map(|idx| idx.into()).collect(),
            )?,
        })
    }
    pub fn range<Index: Into<GenericResourceIndex>>(name: &str, start: Index, end: Index) -> Self {
        GenericResourceDescriptor {
            name: name.to_string(),
            kind: GenericResourceDescriptorKind::Range {
                start: start.into(),
                end: end.into(),
            },
        }
    }
    pub fn sum(name: &str, size: GenericResourceAmount) -> Self {
        GenericResourceDescriptor {
            name: name.to_string(),
            kind: GenericResourceDescriptorKind::Sum { size },
        }
    }
}

/// (Node0(Cpu0, Cpu1), Node1(Cpu2, Cpu3), ...)
pub type CpusDescriptor = Vec<Vec<CpuId>>;

pub fn cpu_descriptor_from_socket_size(
    n_sockets: NumOfCpus,
    n_cpus_per_socket: NumOfCpus,
) -> CpusDescriptor {
    let mut cpu_id_counter = 0;
    (0..n_sockets)
        .map(|_| {
            (0..n_cpus_per_socket)
                .map(|_| {
                    let id = cpu_id_counter;
                    cpu_id_counter += 1;
                    id.into()
                })
                .collect::<Vec<CpuId>>()
        })
        .collect()
}

/// Most precise description of request provided by a worker (without time resource)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceDescriptor {
    pub cpus: CpusDescriptor,
    pub generic: Vec<GenericResourceDescriptor>,
}

impl ResourceDescriptor {
    pub fn new(cpus: CpusDescriptor, mut generic: Vec<GenericResourceDescriptor>) -> Self {
        generic.sort_by(|x, y| x.name.cmp(&y.name));

        ResourceDescriptor { cpus, generic }
    }

    pub fn full_describe(&self) -> String {
        format_comma_delimited(
            self.cpus
                .iter()
                .map(|socket| format!("[{}]", format_comma_delimited(socket))),
        )
    }

    pub fn validate(&self) -> crate::Result<()> {
        if self.cpus.is_empty() || !self.cpus.iter().all(|g| !g.is_empty()) {
            return Err(crate::Error::GenericError("Invalid number of cpus".into()));
        }
        let s: Set<CpuId> = self.cpus.iter().flatten().copied().collect();
        if s.len() != self.cpus.iter().flatten().count() {
            return Err(crate::Error::GenericError(
                "Same CPU id in two sockets".into(),
            ));
        }

        let s: Set<String> = self.generic.iter().map(|g| g.name.clone()).collect();
        if s.len() != self.generic.len() {
            return Err(crate::Error::GenericError(
                "Same resource defined twice".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::common::index::AsIdVec;

    impl ResourceDescriptor {
        pub fn simple(n_cpus: NumOfCpus) -> Self {
            Self::new(
                cpu_descriptor_from_socket_size(1, n_cpus),
                Default::default(),
            )
        }
    }

    #[test]
    fn test_resources_to_describe() {
        let d = ResourceDescriptor::new(vec![vec![0].to_ids()], Vec::new());
        assert_eq!(&d.full_describe(), "[0]");

        let d = ResourceDescriptor::new(
            vec![vec![0, 1, 2, 4].to_ids(), vec![10, 11, 12, 14].to_ids()],
            Vec::new(),
        );
        assert_eq!(&d.full_describe(), "[0,1,2,4],[10,11,12,14]");
    }
}
