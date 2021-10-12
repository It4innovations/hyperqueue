use crate::common::resources::{CpuId, GenericResourceAmount, GenericResourceIndex, NumOfCpus};
use crate::common::{Map, Set};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenericResourceKindIndices {
    pub start: GenericResourceIndex,
    pub end: GenericResourceIndex,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenericResourceKindSum {
    pub size: GenericResourceAmount,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum GenericResourceDescriptorKind {
    Indices(GenericResourceKindIndices),
    // TODO: Named(Vec<String>),
    Sum(GenericResourceKindSum),
}

impl GenericResourceDescriptorKind {
    pub fn size(&self) -> GenericResourceAmount {
        match self {
            GenericResourceDescriptorKind::Indices(idx) if idx.end >= idx.start => {
                (idx.end + 1 - idx.start) as u64
            }
            GenericResourceDescriptorKind::Indices(_) => 0,
            GenericResourceDescriptorKind::Sum(x) => x.size,
        }
    }
}

impl std::fmt::Display for GenericResourceDescriptorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GenericResourceDescriptorKind::Indices(idx) => {
                write!(f, "Indices({}-{})", idx.start, idx.end)
            }
            GenericResourceDescriptorKind::Sum(v) => write!(f, "Sum({})", v.size),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenericResourceDescriptor {
    pub name: String,
    pub kind: GenericResourceDescriptorKind,
}

impl GenericResourceDescriptor {
    #[cfg(test)]
    pub fn indices(name: &str, count: GenericResourceIndex) -> Self {
        GenericResourceDescriptor {
            name: name.to_string(),
            kind: GenericResourceDescriptorKind::Indices(1, count),
        }
    }
    #[cfg(test)]
    pub fn sum(name: &str, amount: GenericResourceAmount) -> Self {
        GenericResourceDescriptor {
            name: name.to_string(),
            kind: GenericResourceDescriptorKind::Sum(amount),
        }
    }
}

pub type CpusDescriptor = Vec<Vec<CpuId>>;

/// Most precise description of request provided by a worker (without time resource)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceDescriptor {
    pub cpus: CpusDescriptor,
    pub generic: Vec<GenericResourceDescriptor>,
}

impl ResourceDescriptor {
    #[cfg(test)]
    pub fn simple(n_cpus: NumOfCpus) -> Self {
        ResourceDescriptor::new_with_socket_size(1, n_cpus)
    }

    pub fn new_with_cpus(cpus: CpusDescriptor) -> Self {
        ResourceDescriptor {
            cpus,
            generic: Default::default(),
        }
    }

    pub fn new_with_socket_size(n_sockets: NumOfCpus, n_cpus_per_socket: NumOfCpus) -> Self {
        let mut cpu_id_counter = 0;
        let cpus = (0..n_sockets)
            .map(|_| {
                (0..n_cpus_per_socket)
                    .map(|_| {
                        let id = cpu_id_counter;
                        cpu_id_counter += 1;
                        id
                    })
                    .collect::<Vec<CpuId>>()
            })
            .collect();
        Self::new_with_cpus(cpus)
    }

    pub fn add_generic_resource(&mut self, descriptor: GenericResourceDescriptor) {
        assert!(self
            .generic
            .iter()
            .find(|d| d.name == descriptor.name)
            .is_none());
        self.generic.push(descriptor);
    }

    pub fn normalize(&mut self) {
        self.generic.sort_by(|x, y| x.name.cmp(&y.name))
    }

    pub fn validate(&self) -> bool {
        if self.cpus.is_empty() || !self.cpus.iter().all(|g| !g.is_empty()) {
            return false;
        }
        let s: Set<CpuId> = self.cpus.iter().flatten().copied().collect();
        s.len() == self.cpus.iter().flatten().count()
    }

    pub fn full_describe(&self) -> String {
        self.cpus
            .iter()
            .map(|socket| {
                format!(
                    "[{}]",
                    socket
                        .iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn summary(&self, multiline: bool) -> String {
        let mut result = if self.cpus.len() == 1 {
            format!("1x{} cpus", self.cpus[0].len())
        } else {
            let mut counts = Map::<usize, usize>::new();
            for group in &self.cpus {
                *counts.entry(group.len()).or_default() += 1;
            }
            let mut counts: Vec<_> = counts.into_iter().collect();
            counts.sort_unstable();
            format!(
                "{} cpus",
                counts
                    .iter()
                    .map(|(cores, count)| format!("{}x{}", count, cores))
                    .collect::<Vec<_>>()
                    .join(" ")
            )
        };
        if multiline {
            for descriptor in &self.generic {
                result.push_str(&format!("\n{}: {}", &descriptor.name, descriptor.kind));
            }
        } else {
            for descriptor in &self.generic {
                result.push_str(&format!(
                    "; {} {}",
                    &descriptor.name,
                    descriptor.kind.size()
                ));
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resources_to_summary() {
        let d = ResourceDescriptor::new_with_cpus(vec![vec![0]]);
        assert_eq!(&d.summary(), "1x1 cpus");

        let d = ResourceDescriptor::new_with_cpus(vec![vec![0, 1, 2]]);
        assert_eq!(&d.summary(), "1x3 cpus");

        let d = ResourceDescriptor::new_with_cpus(vec![vec![0, 1, 2, 4], vec![10, 11, 12, 14]]);
        assert_eq!(&d.summary(), "2x4 cpus");

        let d = ResourceDescriptor::new_with_cpus(vec![
            vec![0, 1],
            vec![10, 11],
            vec![20, 21],
            vec![30, 31],
            vec![40, 41],
            vec![50, 51, 52, 53, 54, 55],
        ]);
        assert_eq!(&d.summary(), "5x2 1x6 cpus");

        let mut d = ResourceDescriptor::new_with_cpus(vec![vec![0, 1]]);
        d.add_generic_resource(GenericResourceDescriptor {
            name: "Aaa".to_string(),
            kind: GenericResourceDescriptorKind::Indices(0, 9),
        });
        d.add_generic_resource(GenericResourceDescriptor {
            name: "Ccc".to_string(),
            kind: GenericResourceDescriptorKind::Indices(1, 132),
        });
        d.add_generic_resource(GenericResourceDescriptor {
            name: "Bbb".to_string(),
            kind: GenericResourceDescriptorKind::Sum(100_000_000),
        });
        d.normalize();
        assert_eq!(
            &d.summary(),
            "1x2 cpus\nAaa = Indices(0-9)\nBbb = Sum(100000000)\nCcc = Indices(1-132)"
        );
    }

    #[test]
    fn test_resources_to_describe() {
        let d = ResourceDescriptor::new_with_cpus(vec![vec![0]]);
        assert_eq!(&d.full_describe(), "[0]");

        let d = ResourceDescriptor::new_with_cpus(vec![vec![0, 1, 2, 4], vec![10, 11, 12, 14]]);
        assert_eq!(&d.full_describe(), "[0, 1, 2, 4], [10, 11, 12, 14]");
    }
}
