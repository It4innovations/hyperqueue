use crate::common::Set;
use crate::messages::gateway::StopWorkerRequest;
use serde::{Deserialize, Serialize};

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

    /*pub fn set_n_cpus(&mut self, n_cpus: NumOfCpus) {
        self.n_cpus = n_cpus;
    }*/

    /*#[inline]
    pub fn get_n_cpus(&self) -> NumOfCpus {
        self.n_cpus
    }*/
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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceDescriptor {
    pub cpus: Vec<Vec<CpuId>>,
}

impl ResourceDescriptor {
    #[cfg(test)]
    pub fn simple(n_cpus: NumOfCpus) -> Self {
        ResourceDescriptor {
            cpus: vec![(0..n_cpus).collect()],
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
        ResourceDescriptor { cpus }
    }

    pub fn validate(&self) -> bool {
        if self.cpus.is_empty() || !self.cpus.iter().all(|g| !g.is_empty()) {
            return false;
        }
        let mut s: Set<CpuId> = self.cpus.iter().flatten().copied().collect();
        s.len() == self.cpus.iter().flatten().count()
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
}
