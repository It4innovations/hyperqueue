use serde::{Deserialize, Serialize};

use crate::common::error::DsError;
use crate::common::{Map, Set};

pub type NumOfCpus = u32;
pub type CpuId = u32;

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub enum CpuRequest {
    Compact(NumOfCpus),
    ForceCompact(NumOfCpus),
    Scatter(NumOfCpus),
    All,
}

impl Default for CpuRequest {
    fn default() -> Self {
        CpuRequest::Compact(1)
    }
}

impl CpuRequest {
    pub fn validate(&self) -> crate::Result<()> {
        match &self {
            CpuRequest::Scatter(n_cpus)
            | CpuRequest::ForceCompact(n_cpus)
            | CpuRequest::Compact(n_cpus) => {
                if *n_cpus == 0 {
                    Err(DsError::GenericError(
                        "Zero cpus cannot be requested".to_string(),
                    ))
                } else {
                    Ok(())
                }
            }
            CpuRequest::All => Ok(()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct ResourceRequest {
    cpus: CpuRequest,
}

impl Default for ResourceRequest {
    fn default() -> Self {
        ResourceRequest {
            cpus: CpuRequest::default(),
        }
    }
}

impl ResourceRequest {
    pub fn new(cpu_request: CpuRequest) -> ResourceRequest {
        ResourceRequest { cpus: cpu_request }
    }

    pub fn cpus(&self) -> &CpuRequest {
        &self.cpus
    }

    #[cfg(test)]
    pub fn set_cpus(&mut self, cpus: CpuRequest) {
        self.cpus = cpus;
    }

    pub fn sort_key(&self) -> (NumOfCpus, NumOfCpus) {
        match &self.cpus {
            CpuRequest::Compact(n_cpus) => (*n_cpus, 1),
            CpuRequest::ForceCompact(n_cpus) => (*n_cpus, 2),
            CpuRequest::Scatter(n_cpus) => (*n_cpus, 0),
            CpuRequest::All => (NumOfCpus::MAX, NumOfCpus::MAX),
        }
    }

    pub fn validate(&self) -> crate::Result<()> {
        self.cpus.validate()
    }
}

pub type CpusDescriptor = Vec<Vec<CpuId>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceDescriptor {
    pub cpus: Vec<Vec<CpuId>>,
}

impl ResourceDescriptor {
    #[cfg(test)]
    pub fn simple(n_cpus: NumOfCpus) -> Self {
        ResourceDescriptor::new_with_socket_size(1, n_cpus)
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
        ResourceDescriptor { cpus }
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

    pub fn summary(&self) -> String {
        if self.cpus.len() == 1 {
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
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ResourceAllocation {
    pub cpus: Vec<CpuId>,
}

impl ResourceAllocation {
    pub fn new(cpus: Vec<CpuId>) -> Self {
        ResourceAllocation { cpus }
    }

    pub fn comma_delimited_cpu_ids(&self) -> String {
        self.cpus
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resources_to_summary() {
        let d = ResourceDescriptor {
            cpus: vec![vec![0]],
        };
        assert_eq!(&d.summary(), "1x1 cpus");

        let d = ResourceDescriptor {
            cpus: vec![vec![0, 1, 2]],
        };
        assert_eq!(&d.summary(), "1x3 cpus");

        let d = ResourceDescriptor {
            cpus: vec![vec![0, 1, 2, 4], vec![10, 11, 12, 14]],
        };
        assert_eq!(&d.summary(), "2x4 cpus");

        let d = ResourceDescriptor {
            cpus: vec![
                vec![0, 1],
                vec![10, 11],
                vec![20, 21],
                vec![30, 31],
                vec![40, 41],
                vec![50, 51, 52, 53, 54, 55],
            ],
        };
        assert_eq!(&d.summary(), "5x2 1x6 cpus");
    }

    #[test]
    fn test_resources_to_describe() {
        let d = ResourceDescriptor {
            cpus: vec![vec![0]],
        };
        assert_eq!(&d.full_describe(), "[0]");

        let d = ResourceDescriptor {
            cpus: vec![vec![0, 1, 2, 4], vec![10, 11, 12, 14]],
        };
        assert_eq!(&d.full_describe(), "[0, 1, 2, 4], [10, 11, 12, 14]");
    }
}
