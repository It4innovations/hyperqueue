use crate::common::resources::{
    CpuId, CpuRequest, NumOfCpus, ResourceAllocation, ResourceDescriptor, ResourceRequest,
};
use crate::common::Map;

pub type SocketId = CpuId;

pub struct ResourcePool {
    free_cpus: Vec<Vec<CpuId>>,
    cpu_id_to_socket: Map<CpuId, SocketId>,
    socket_size: NumOfCpus,
}

impl ResourcePool {
    pub fn new(desc: &ResourceDescriptor) -> Self {
        assert!(desc.validate());
        let cpu_ids = desc
            .cpus
            .iter()
            .enumerate()
            .map(|(i, c)| c.iter().map(move |v| (*v as CpuId, i as SocketId)))
            .flatten()
            .collect();
        let socket_size = desc.cpus.iter().map(|v| v.len()).max().unwrap() as NumOfCpus;
        ResourcePool {
            free_cpus: desc.cpus.clone(),
            cpu_id_to_socket: cpu_ids,
            socket_size,
        }
    }

    pub fn n_free_cpus(&self) -> NumOfCpus {
        self.free_cpus
            .iter()
            .map(|group| group.len())
            .sum::<usize>() as NumOfCpus
    }

    pub fn release_allocation(&mut self, allocation: ResourceAllocation) {
        for cpu_id in allocation.cpus {
            let socket_id = *self.cpu_id_to_socket.get(&cpu_id).unwrap();
            self.free_cpus[socket_id as usize].push(cpu_id);
        }
    }

    pub fn n_cpus(&self, request: &ResourceRequest) -> NumOfCpus {
        match request.cpus() {
            CpuRequest::Compact(n_cpus)
            | CpuRequest::ForceCompact(n_cpus)
            | CpuRequest::Scatter(n_cpus) => *n_cpus,
            CpuRequest::All => self.cpu_id_to_socket.len() as NumOfCpus,
        }
    }

    fn _take_all_free_cpus(&mut self) -> Vec<CpuId> {
        let mut cpus = Vec::new();
        for group in &mut self.free_cpus {
            cpus.append(group);
        }
        cpus
    }

    fn _try_allocate_resources_compact(
        &mut self,
        mut n_cpus: NumOfCpus,
        total_cpus: NumOfCpus,
    ) -> Option<Vec<CpuId>> {
        if total_cpus == n_cpus {
            Some(self._take_all_free_cpus())
        } else {
            let mut cpus = Vec::new();
            while n_cpus > 0 {
                if let Some(group) = self
                    .free_cpus
                    .iter_mut()
                    .filter(|group| group.len() >= n_cpus as usize)
                    .min_by_key(|group| group.len())
                {
                    for _ in 0..n_cpus {
                        cpus.push(group.pop().unwrap());
                    }
                    break;
                } else {
                    let group = self
                        .free_cpus
                        .iter_mut()
                        .max_by_key(|group| group.len())
                        .unwrap();
                    n_cpus -= group.len() as NumOfCpus;
                    cpus.append(group);
                }
            }
            Some(cpus)
        }
    }

    fn _try_allocate_resources_scatter(
        &mut self,
        mut n_cpus: NumOfCpus,
        total_cpus: NumOfCpus,
    ) -> Option<Vec<CpuId>> {
        if total_cpus == n_cpus {
            Some(self._take_all_free_cpus())
        } else {
            let count = self.free_cpus.len();
            let mut cpus = Vec::new();
            let mut i = self
                .free_cpus
                .iter()
                .enumerate()
                .max_by_key(|(_, group)| group.len())
                .unwrap()
                .0;
            while n_cpus > 0 {
                if let Some(cpu_id) = self.free_cpus[i % count].pop() {
                    cpus.push(cpu_id);
                    n_cpus -= 1;
                }
                i += 1;
            }
            Some(cpus)
        }
    }

    fn _try_allocate_resources_force_compact(
        &mut self,
        mut n_cpus: NumOfCpus,
    ) -> Option<Vec<CpuId>> {
        let mut full_sockets = n_cpus / self.socket_size;
        let free_sockets = self
            .free_cpus
            .iter()
            .filter(|g| g.len() as NumOfCpus == self.socket_size)
            .count() as NumOfCpus;
        if free_sockets < full_sockets {
            return None;
        }
        let remainder = n_cpus % self.socket_size as NumOfCpus;
        let mut cpus = Vec::new();
        if remainder > 0 {
            let group_idx = if free_sockets == full_sockets {
                self.free_cpus
                    .iter()
                    .enumerate()
                    .filter(|(_, g)| {
                        let len = g.len() as NumOfCpus;
                        len < self.socket_size && len >= remainder
                    })
                    .min_by_key(|(_, g)| g.len())
            } else {
                self.free_cpus
                    .iter()
                    .enumerate()
                    .filter(|(_, g)| {
                        let len = g.len() as NumOfCpus;
                        len <= self.socket_size && len >= remainder
                    })
                    .min_by_key(|(_, g)| g.len())
            }?
            .0;
            for _ in 0..remainder {
                cpus.push(self.free_cpus[group_idx].pop().unwrap());
            }
        }

        if full_sockets > 0 {
            for group in &mut self.free_cpus {
                if group.len() as NumOfCpus == self.socket_size {
                    cpus.append(group);
                    full_sockets -= 1;
                    if full_sockets == 0 {
                        break;
                    }
                }
            }
        }
        debug_assert!(full_sockets == 0);
        Some(cpus)
    }

    pub fn try_allocate_resources(
        &mut self,
        request: &ResourceRequest,
    ) -> Option<ResourceAllocation> {
        let mut n_cpus = self.n_cpus(&request);
        let total_cpus = self.n_free_cpus();
        if total_cpus < n_cpus {
            return None;
        }
        let mut cpus = match request.cpus() {
            CpuRequest::Compact(_) => self._try_allocate_resources_compact(n_cpus, total_cpus)?,
            CpuRequest::ForceCompact(_) => self._try_allocate_resources_force_compact(n_cpus)?,
            CpuRequest::Scatter(_) => self._try_allocate_resources_scatter(n_cpus, total_cpus)?,
            CpuRequest::All => self._take_all_free_cpus(),
        };
        cpus.sort_unstable();
        Some(ResourceAllocation::new(cpus))
    }
}

#[cfg(test)]
mod tests {
    use crate::common::resources::{
        CpuId, CpuRequest, ResourceAllocation, ResourceDescriptor, ResourceRequest,
    };
    use crate::common::Set;
    use crate::server::test_util::sorted_vec;
    use crate::worker::pool::{ResourcePool, SocketId};

    impl ResourcePool {
        fn get_sockets(&self, allocation: &ResourceAllocation) -> Vec<SocketId> {
            sorted_vec(
                allocation
                    .cpus
                    .iter()
                    .map(|id| *self.cpu_id_to_socket.get(&id).unwrap())
                    .collect::<Set<_>>()
                    .into_iter()
                    .collect(),
            )
        }
    }

    #[test]
    fn test_pool_single_socket() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(1, 4));

        let rq = ResourceRequest::new(CpuRequest::Compact(2));
        let al = pool.try_allocate_resources(&rq).unwrap();

        assert_eq!(pool.n_free_cpus(), 2);
        assert_eq!(al.cpus.len(), 2);
        assert!(al.cpus[0] < al.cpus[1]);
        assert!(0 < al.cpus[0] && al.cpus[0] < al.cpus[1] && al.cpus[1] < 4);

        let rq2 = ResourceRequest::new(CpuRequest::Compact(4));
        assert!(pool.try_allocate_resources(&rq2).is_none());
        let rq2 = ResourceRequest::new(CpuRequest::Compact(3));
        assert!(pool.try_allocate_resources(&rq2).is_none());

        pool.release_allocation(al);
        assert_eq!(pool.n_free_cpus(), 4);

        let rq = ResourceRequest::new(CpuRequest::Compact(4));
        let al = pool.try_allocate_resources(&rq).unwrap();
        assert_eq!(al.cpus, vec![0, 1, 2, 3]);

        assert_eq!(pool.n_free_cpus(), 0);

        pool.release_allocation(al);

        assert_eq!(pool.n_free_cpus(), 4);

        let rq = ResourceRequest::new(CpuRequest::Compact(1));
        let rq2 = ResourceRequest::new(CpuRequest::Compact(2));
        let rq3 = ResourceRequest::new(CpuRequest::Compact(3));
        let al1 = pool.try_allocate_resources(&rq).unwrap();
        assert_eq!(pool.n_free_cpus(), 3);
        let al2 = pool.try_allocate_resources(&rq).unwrap();
        assert_eq!(pool.n_free_cpus(), 2);
        assert!(pool.try_allocate_resources(&rq3).is_none());
        let al3 = pool.try_allocate_resources(&rq2).unwrap();
        assert_eq!(pool.n_free_cpus(), 0);
        assert!(pool.try_allocate_resources(&rq).is_none());

        assert_eq!(al1.cpus.len(), 1);
        assert_eq!(al2.cpus.len(), 1);
        assert_ne!(al1.cpus, al2.cpus);
        assert_eq!(al3.cpus.len(), 2);
    }

    #[test]
    fn test_pool_compact1() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(4, 6));

        let rq1 = ResourceRequest::new(CpuRequest::Compact(4));
        let al1 = pool.try_allocate_resources(&rq1).unwrap();
        assert_eq!(pool.get_sockets(&al1).len(), 1);
        let al2 = pool.try_allocate_resources(&rq1).unwrap();
        assert_eq!(pool.get_sockets(&al2).len(), 1);
        assert_ne!(pool.get_sockets(&al1), pool.get_sockets(&al2));

        let rq2 = ResourceRequest::new(CpuRequest::Compact(3));

        let al3 = pool.try_allocate_resources(&rq2).unwrap();
        assert_eq!(pool.get_sockets(&al3).len(), 1);
        let al4 = pool.try_allocate_resources(&rq2).unwrap();
        assert_eq!(pool.get_sockets(&al4).len(), 1);
        assert_eq!(pool.get_sockets(&al3), pool.get_sockets(&al4));

        let rq3 = ResourceRequest::new(CpuRequest::Compact(6));
        let al = pool.try_allocate_resources(&rq3).unwrap();
        assert_eq!(pool.get_sockets(&al).len(), 1);
        pool.release_allocation(al);

        let rq3 = ResourceRequest::new(CpuRequest::Compact(7));
        let al = pool.try_allocate_resources(&rq3).unwrap();
        assert_eq!(pool.get_sockets(&al).len(), 2);
        pool.release_allocation(al);

        let rq3 = ResourceRequest::new(CpuRequest::Compact(8));
        let al = pool.try_allocate_resources(&rq3).unwrap();
        assert_eq!(pool.get_sockets(&al).len(), 2);
        pool.release_allocation(al);

        let rq3 = ResourceRequest::new(CpuRequest::Compact(9));
        let al = pool.try_allocate_resources(&rq3).unwrap();
        assert_eq!(pool.get_sockets(&al).len(), 3);
        pool.release_allocation(al);
    }

    #[test]
    fn test_pool_allocate_compact_all() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(4, 6));

        let rq = ResourceRequest::new(CpuRequest::Compact(24));
        let al = pool.try_allocate_resources(&rq).unwrap();
        assert_eq!(al.cpus, (0..24).collect::<Vec<_>>());
        assert_eq!(pool.n_free_cpus(), 0);
        pool.release_allocation(al);
        assert_eq!(pool.n_free_cpus(), 24);
    }

    #[test]
    fn test_pool_allocate_all() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(4, 6));

        let rq = ResourceRequest::new(CpuRequest::All);
        let al = pool.try_allocate_resources(&rq).unwrap();
        assert_eq!(al.cpus, (0..24).collect::<Vec<_>>());
        assert_eq!(pool.n_free_cpus(), 0);
        pool.release_allocation(al);
        assert_eq!(pool.n_free_cpus(), 24);

        let rq2 = ResourceRequest::new(CpuRequest::Compact(1));
        assert!(pool.try_allocate_resources(&rq).is_some());
        assert!(pool.try_allocate_resources(&rq).is_none());
    }

    #[test]
    fn test_pool_force_compact1() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(2, 4));

        let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(9));
        assert!(pool.try_allocate_resources(&rq1).is_none());

        for _ in 0..4 {
            let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(2));
            let al1 = pool.try_allocate_resources(&rq1).unwrap();
            assert_eq!(al1.cpus.len(), 2);
            assert_eq!(pool.get_sockets(&al1).len(), 1);
        }

        let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(2));
        assert!(pool.try_allocate_resources(&rq1).is_none());
    }

    #[test]
    fn test_pool_force_compact2() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(2, 4));

        for _ in 0..2 {
            let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(3));
            let al1 = pool.try_allocate_resources(&rq1).unwrap();
            assert_eq!(al1.cpus.len(), 3);
            assert_eq!(pool.get_sockets(&al1).len(), 1);
        }

        let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(2));
        assert!(pool.try_allocate_resources(&rq1).is_none());

        let rq1 = ResourceRequest::new(CpuRequest::Compact(2));
        assert!(pool.try_allocate_resources(&rq1).is_some());
    }

    #[test]
    fn test_pool_force_compact3() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(3, 4));

        let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(8));
        let al1 = pool.try_allocate_resources(&rq1).unwrap();
        assert_eq!(al1.cpus.len(), 8);
        assert_eq!(pool.get_sockets(&al1).len(), 2);
        pool.release_allocation(al1);

        let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(5));
        let al1 = pool.try_allocate_resources(&rq1).unwrap();
        assert_eq!(al1.cpus.len(), 5);
        assert_eq!(pool.get_sockets(&al1).len(), 2);
        pool.release_allocation(al1);

        let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(10));
        let al1 = pool.try_allocate_resources(&rq1).unwrap();
        assert_eq!(al1.cpus.len(), 10);
        assert_eq!(pool.get_sockets(&al1).len(), 3);
        pool.release_allocation(al1);
    }

    #[test]
    fn test_pool_force_scatter1() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(3, 4));

        let rq1 = ResourceRequest::new(CpuRequest::Scatter(3));
        let al1 = pool.try_allocate_resources(&rq1).unwrap();
        assert_eq!(al1.cpus.len(), 3);
        assert_eq!(pool.get_sockets(&al1).len(), 3);

        let rq1 = ResourceRequest::new(CpuRequest::Scatter(4));
        let al1 = pool.try_allocate_resources(&rq1).unwrap();
        assert_eq!(al1.cpus.len(), 4);
        assert_eq!(pool.get_sockets(&al1).len(), 3);
    }

    #[test]
    fn test_pool_force_scatter2() {
        let mut pool = ResourcePool::new(&ResourceDescriptor::new_with_socket_size(3, 4));

        let rq1 = ResourceRequest::new(CpuRequest::ForceCompact(4));
        pool.try_allocate_resources(&rq1).unwrap();

        let rq1 = ResourceRequest::new(CpuRequest::Scatter(5));
        let al1 = pool.try_allocate_resources(&rq1).unwrap();
        assert_eq!(al1.cpus.len(), 5);
        assert_eq!(pool.get_sockets(&al1).len(), 2);
    }
}
