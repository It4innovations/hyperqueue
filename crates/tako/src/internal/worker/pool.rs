use crate::internal::common::index::IndexVec;
use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::resources::{
    CpuId, CpuRequest, GenericResourceAllocation, GenericResourceAllocationValue,
    GenericResourceAllocations, GenericResourceAmount, GenericResourceDescriptorKind,
    GenericResourceIndex, GenericResourceRequest, NumOfCpus, ResourceAllocation,
    ResourceDescriptor, ResourceRequest, ResourceVec, SocketId,
};
use crate::internal::common::Map;
use std::time::Duration;

#[derive(Debug)]
pub enum GenericResourcePool {
    Empty,
    Indices(Vec<GenericResourceIndex>),
    Sum(GenericResourceAmount),
}

impl GenericResourcePool {
    pub fn size(&self) -> GenericResourceAmount {
        match self {
            GenericResourcePool::Empty => 0,
            GenericResourcePool::Indices(indices) => indices.len() as GenericResourceAmount,
            GenericResourcePool::Sum(size) => *size,
        }
    }

    pub fn claim_resources(
        &mut self,
        amount: GenericResourceAmount,
    ) -> GenericResourceAllocationValue {
        match self {
            GenericResourcePool::Empty => unreachable!(),
            GenericResourcePool::Indices(ref mut indices) => {
                GenericResourceAllocationValue::new_indices(
                    (0..amount).map(|_| indices.pop().unwrap()).collect(),
                )
            }
            GenericResourcePool::Sum(ref mut size) => {
                assert!(*size >= amount);
                *size -= amount;
                GenericResourceAllocationValue::new_sum(amount)
            }
        }
    }

    pub fn release_allocation(&mut self, allocation: GenericResourceAllocationValue) {
        match self {
            GenericResourcePool::Empty => unreachable!(),
            GenericResourcePool::Indices(ref mut indices) => match allocation {
                GenericResourceAllocationValue::Indices(alloc_indices) => {
                    indices.extend_from_slice(&alloc_indices[..]);
                }
                _ => unreachable!(),
            },
            GenericResourcePool::Sum(ref mut size) => match allocation {
                GenericResourceAllocationValue::Sum(amount) => {
                    *size += amount;
                }
                _ => unreachable!(),
            },
        };
    }
}

pub struct ResourcePool {
    free_cpus: IndexVec<SocketId, Vec<CpuId>>,
    cpu_id_to_socket: Map<CpuId, SocketId>,
    socket_size: NumOfCpus,

    free_generic_resources: ResourceVec<GenericResourcePool>,
    generic_resource_sizes: ResourceVec<GenericResourceAmount>,
}

impl ResourcePool {
    pub fn new(desc: &ResourceDescriptor, resource_map: &ResourceMap) -> Self {
        /* Construct CPU pool */
        desc.validate().unwrap();
        let cpu_ids = desc
            .cpus
            .iter()
            .enumerate()
            .flat_map(|(i, c)| c.iter().map(move |&v| (v, SocketId::new(i as u32))))
            .collect();
        let socket_size = desc.cpus.iter().map(|v| v.len()).max().unwrap() as NumOfCpus;

        /* Construct generic resource pool */
        let mut generic_resources = Vec::new();
        generic_resources.resize_with(resource_map.len(), || GenericResourcePool::Empty);

        for descriptor in &desc.generic {
            let idx = resource_map
                .get_index(&descriptor.name)
                .expect("Internal error, resource name not received")
                .as_num() as usize;
            generic_resources[idx] = match &descriptor.kind {
                GenericResourceDescriptorKind::List { values } => {
                    GenericResourcePool::Indices(values.clone())
                }
                GenericResourceDescriptorKind::Range { start, end } => {
                    GenericResourcePool::Indices(
                        (start.as_num()..=end.as_num())
                            .map(|id| id.into())
                            .collect(),
                    )
                }
                GenericResourceDescriptorKind::Sum { size } => GenericResourcePool::Sum(*size),
            }
        }

        let sizes: Vec<_> = generic_resources.iter().map(|pool| pool.size()).collect();

        ResourcePool {
            free_cpus: desc.cpus.clone().into(),
            cpu_id_to_socket: cpu_ids,
            socket_size,
            free_generic_resources: generic_resources.into(),
            generic_resource_sizes: sizes.into(),
        }
    }

    pub fn fraction_of_resource(&self, generic_request: &GenericResourceRequest) -> f32 {
        let size = self.generic_resource_sizes[generic_request.resource];
        if size == 0 {
            0.0f32
        } else {
            generic_request.amount as f32 / size as f32
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
            self.free_cpus[socket_id].push(cpu_id);
        }
        for ga in allocation.generic_allocations {
            self.free_generic_resources[ga.resource].release_allocation(ga.value);
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
        for group in self.free_cpus.iter_mut() {
            cpus.append(group);
        }
        cpus
    }

    fn _try_allocate_resources_compact(
        &mut self,
        mut n_cpus: NumOfCpus,
        total_cpus: NumOfCpus,
    ) -> Vec<CpuId> {
        if total_cpus == n_cpus {
            self._take_all_free_cpus()
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
            cpus
        }
    }

    fn _try_allocate_resources_scatter(
        &mut self,
        mut n_cpus: NumOfCpus,
        total_cpus: NumOfCpus,
    ) -> Vec<CpuId> {
        if total_cpus == n_cpus {
            self._take_all_free_cpus()
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
                if let Some(cpu_id) = self.free_cpus[SocketId::new((i % count) as u32)].pop() {
                    cpus.push(cpu_id);
                    n_cpus -= 1;
                }
                i += 1;
            }
            cpus
        }
    }

    fn _try_allocate_resources_force_compact(&mut self, n_cpus: NumOfCpus) -> Option<Vec<CpuId>> {
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
                cpus.push(
                    self.free_cpus[SocketId::new(group_idx as u32)]
                        .pop()
                        .unwrap(),
                );
            }
        }

        if full_sockets > 0 {
            for group in self.free_cpus.iter_mut() {
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
        remaining_time: Option<Duration>,
    ) -> Option<ResourceAllocation> {
        let n_cpus = self.n_cpus(request);
        let total_cpus = self.n_free_cpus();
        if total_cpus < n_cpus {
            return None;
        }
        if let Some(remaining_time) = remaining_time {
            if remaining_time < request.min_time() {
                return None;
            }
        }

        let empty = GenericResourcePool::Empty;

        for gr in request.generic_requests() {
            let pool: &GenericResourcePool = self
                .free_generic_resources
                .get(gr.resource.as_num() as usize)
                .unwrap_or(&empty);
            if pool.size() < gr.amount {
                return None;
            }
        }

        let mut cpus = match request.cpus() {
            CpuRequest::Compact(_) => self._try_allocate_resources_compact(n_cpus, total_cpus),
            CpuRequest::ForceCompact(_) => self._try_allocate_resources_force_compact(n_cpus)?,
            CpuRequest::Scatter(_) => self._try_allocate_resources_scatter(n_cpus, total_cpus),
            CpuRequest::All => self._take_all_free_cpus(),
        };
        cpus.sort_unstable();
        let mut generic_allocations = GenericResourceAllocations::new();

        for gr in request.generic_requests() {
            let pool: &mut GenericResourcePool = self
                .free_generic_resources
                .get_mut(gr.resource.as_num() as usize)
                .unwrap();
            generic_allocations.push(GenericResourceAllocation {
                resource: gr.resource,
                value: pool.claim_resources(gr.amount),
            })
        }
        Some(ResourceAllocation::new(
            Vec::new(),
            cpus,
            generic_allocations,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::index::AsIdVec;
    use crate::internal::common::resources::descriptor::cpu_descriptor_from_socket_size;
    use crate::internal::common::resources::map::ResourceMap;
    use crate::internal::common::resources::{
        GenericResourceAllocationValue, GenericResourceDescriptor, ResourceAllocation,
        ResourceDescriptor, ResourceRequest,
    };
    use crate::internal::common::Set;
    use crate::internal::tests::utils::resources::{
        cpus_all, cpus_compact, cpus_force_compact, cpus_scatter,
    };
    use crate::internal::tests::utils::sorted_vec;
    use crate::internal::worker::pool::{GenericResourcePool, ResourcePool, SocketId};

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
        let cores = cpu_descriptor_from_socket_size(1, 4);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cores, Vec::new()),
            &Default::default(),
        );

        let rq = cpus_compact(2).finish();
        let al = pool.try_allocate_resources(&rq, None).unwrap();

        assert_eq!(pool.n_free_cpus(), 2);
        assert_eq!(al.cpus.len(), 2);
        assert!(al.cpus[0] < al.cpus[1]);
        assert!(0 < al.cpus[0].as_num() && al.cpus[0] < al.cpus[1] && al.cpus[1].as_num() < 4);

        let rq2 = cpus_compact(4).finish();
        assert!(pool.try_allocate_resources(&rq2, None).is_none());
        let rq2 = cpus_compact(3).finish();
        assert!(pool.try_allocate_resources(&rq2, None).is_none());

        pool.release_allocation(al);
        assert_eq!(pool.n_free_cpus(), 4);

        let rq = cpus_compact(4).finish();
        let al = pool.try_allocate_resources(&rq, None).unwrap();
        assert_eq!(al.cpus, vec![0, 1, 2, 3].to_ids());

        assert_eq!(pool.n_free_cpus(), 0);

        pool.release_allocation(al);

        assert_eq!(pool.n_free_cpus(), 4);

        let rq = cpus_compact(1).finish();
        let rq2 = cpus_compact(2).finish();
        let rq3 = cpus_compact(3).finish();
        let al1 = pool.try_allocate_resources(&rq, None).unwrap();
        assert_eq!(pool.n_free_cpus(), 3);
        let al2 = pool.try_allocate_resources(&rq, None).unwrap();
        assert_eq!(pool.n_free_cpus(), 2);
        assert!(pool.try_allocate_resources(&rq3, None).is_none());
        let al3 = pool.try_allocate_resources(&rq2, None).unwrap();
        assert_eq!(pool.n_free_cpus(), 0);
        assert!(pool.try_allocate_resources(&rq, None).is_none());

        assert_eq!(al1.cpus.len(), 1);
        assert_eq!(al2.cpus.len(), 1);
        assert_ne!(al1.cpus, al2.cpus);
        assert_eq!(al3.cpus.len(), 2);
    }

    #[test]
    fn test_pool_compact1() {
        let cpus = cpu_descriptor_from_socket_size(4, 6);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        );

        let rq1 = cpus_compact(4).finish();
        let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
        assert_eq!(pool.get_sockets(&al1).len(), 1);
        let al2 = pool.try_allocate_resources(&rq1, None).unwrap();
        assert_eq!(pool.get_sockets(&al2).len(), 1);
        assert_ne!(pool.get_sockets(&al1), pool.get_sockets(&al2));

        let rq2 = cpus_compact(3).finish();

        let al3 = pool.try_allocate_resources(&rq2, None).unwrap();
        assert_eq!(pool.get_sockets(&al3).len(), 1);
        let al4 = pool.try_allocate_resources(&rq2, None).unwrap();
        assert_eq!(pool.get_sockets(&al4).len(), 1);
        assert_eq!(pool.get_sockets(&al3), pool.get_sockets(&al4));

        let rq3 = cpus_compact(6).finish();
        let al = pool.try_allocate_resources(&rq3, None).unwrap();
        assert_eq!(pool.get_sockets(&al).len(), 1);
        pool.release_allocation(al);

        let rq3 = cpus_compact(7).finish();
        let al = pool.try_allocate_resources(&rq3, None).unwrap();
        assert_eq!(pool.get_sockets(&al).len(), 2);
        pool.release_allocation(al);

        let rq3 = cpus_compact(8).finish();
        let al = pool.try_allocate_resources(&rq3, None).unwrap();
        assert_eq!(pool.get_sockets(&al).len(), 2);
        pool.release_allocation(al);

        let rq3 = cpus_compact(9).finish();
        let al = pool.try_allocate_resources(&rq3, None).unwrap();
        assert_eq!(pool.get_sockets(&al).len(), 3);
        pool.release_allocation(al);
    }

    #[test]
    fn test_pool_allocate_compact_all() {
        let cpus = cpu_descriptor_from_socket_size(4, 6);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        );

        let rq = cpus_compact(24).finish();
        let al = pool.try_allocate_resources(&rq, None).unwrap();
        assert_eq!(al.cpus, (0..24).map(|id| id.into()).collect::<Vec<_>>());
        assert_eq!(pool.n_free_cpus(), 0);
        pool.release_allocation(al);
        assert_eq!(pool.n_free_cpus(), 24);
    }

    #[test]
    fn test_pool_allocate_all() {
        let cpus = cpu_descriptor_from_socket_size(4, 6);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        );

        let rq = cpus_all().finish();
        let al = pool.try_allocate_resources(&rq, None).unwrap();
        assert_eq!(al.cpus, (0..24).map(|id| id.into()).collect::<Vec<_>>());
        assert_eq!(pool.n_free_cpus(), 0);
        pool.release_allocation(al);
        assert_eq!(pool.n_free_cpus(), 24);

        let rq2 = cpus_compact(1).finish();
        assert!(pool.try_allocate_resources(&rq2, None).is_some());
        assert!(pool.try_allocate_resources(&rq, None).is_none());
    }

    #[test]
    fn test_pool_force_compact1() {
        let cpus = cpu_descriptor_from_socket_size(2, 4);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        );

        let rq1 = cpus_force_compact(9).finish();
        assert!(pool.try_allocate_resources(&rq1, None).is_none());

        for _ in 0..4 {
            let rq1 = cpus_force_compact(2).finish();
            let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
            assert_eq!(al1.cpus.len(), 2);
            assert_eq!(pool.get_sockets(&al1).len(), 1);
        }

        let rq1 = cpus_force_compact(2).finish();
        assert!(pool.try_allocate_resources(&rq1, None).is_none());
    }

    #[test]
    fn test_pool_force_compact2() {
        let cpus = cpu_descriptor_from_socket_size(2, 4);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        );

        for _ in 0..2 {
            let rq1 = cpus_force_compact(3).finish();
            let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
            assert_eq!(al1.cpus.len(), 3);
            assert_eq!(pool.get_sockets(&al1).len(), 1);
        }

        let rq1 = cpus_force_compact(2).finish();
        assert!(pool.try_allocate_resources(&rq1, None).is_none());

        let rq1 = cpus_compact(2).finish();
        assert!(pool.try_allocate_resources(&rq1, None).is_some());
    }

    #[test]
    fn test_pool_force_compact3() {
        let cpus = cpu_descriptor_from_socket_size(3, 4);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        );

        let rq1 = cpus_force_compact(8).finish();
        let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
        assert_eq!(al1.cpus.len(), 8);
        assert_eq!(pool.get_sockets(&al1).len(), 2);
        pool.release_allocation(al1);

        let rq1 = cpus_force_compact(5).finish();
        let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
        assert_eq!(al1.cpus.len(), 5);
        assert_eq!(pool.get_sockets(&al1).len(), 2);
        pool.release_allocation(al1);

        let rq1 = cpus_force_compact(10).finish();
        let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
        assert_eq!(al1.cpus.len(), 10);
        assert_eq!(pool.get_sockets(&al1).len(), 3);
        pool.release_allocation(al1);
    }

    #[test]
    fn test_pool_force_scatter1() {
        let cpus = cpu_descriptor_from_socket_size(3, 4);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        );

        let rq1 = cpus_scatter(3).finish();
        let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
        assert_eq!(al1.cpus.len(), 3);
        assert_eq!(pool.get_sockets(&al1).len(), 3);

        let rq1 = cpus_scatter(4).finish();
        let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
        assert_eq!(al1.cpus.len(), 4);
        assert_eq!(pool.get_sockets(&al1).len(), 3);
    }

    #[test]
    fn test_pool_force_scatter2() {
        let cpus = cpu_descriptor_from_socket_size(3, 4);
        let mut pool = ResourcePool::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        );

        let rq1 = cpus_force_compact(4).finish();
        pool.try_allocate_resources(&rq1, None).unwrap();

        let rq1 = cpus_scatter(5).finish();
        let al1 = pool.try_allocate_resources(&rq1, None).unwrap();
        assert_eq!(al1.cpus.len(), 5);
        assert_eq!(pool.get_sockets(&al1).len(), 2);
    }

    #[test]
    fn test_pool_generic_resources() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let generic = vec![
            GenericResourceDescriptor::range("Res0", 5, 100),
            GenericResourceDescriptor::sum("Res1", 100_000_000),
            GenericResourceDescriptor::range("Res2", 0, 1),
            GenericResourceDescriptor::range("Res3", 0, 1),
        ];
        let descriptor = ResourceDescriptor::new(cpus, generic);

        let mut pool = ResourcePool::new(
            &descriptor,
            &ResourceMap::from_vec(vec![
                "Res0".into(),
                "Res1".into(),
                "Res2".into(),
                "Res3".into(),
            ]),
        );

        assert!(
            matches!(&pool.free_generic_resources[2.into()], GenericResourcePool::Indices(indices) if indices.len() == 2)
        );

        let rq: ResourceRequest = cpus_compact(1)
            .add_generic(3, 1)
            .add_generic(0, 12)
            .add_generic(1, 1000_000)
            .finish();
        rq.validate().unwrap();
        let al = pool.try_allocate_resources(&rq, None).unwrap();
        assert_eq!(al.generic_allocations.len(), 3);
        assert_eq!(al.generic_allocations[0].resource.as_num(), 0);
        assert!(matches!(
            &al.generic_allocations[0].value,
            GenericResourceAllocationValue::Indices(indices) if indices.len() == 12
        ));
        assert_eq!(al.generic_allocations[1].resource.as_num(), 1);
        assert!(matches!(
            &al.generic_allocations[1].value,
            GenericResourceAllocationValue::Sum(1000_000)
        ));
        assert_eq!(al.generic_allocations[2].resource.as_num(), 3);
        assert!(matches!(
            &al.generic_allocations[2].value,
            GenericResourceAllocationValue::Indices(indices) if indices.len() == 1
        ));
        assert!(
            matches!(&pool.free_generic_resources[0.into()], GenericResourcePool::Indices(indices) if indices.len() == 84)
        );
        assert!(matches!(
            pool.free_generic_resources[1.into()],
            GenericResourcePool::Sum(99_000_000)
        ));
        assert!(
            matches!(&pool.free_generic_resources[2.into()], GenericResourcePool::Indices(indices) if indices.len() == 2)
        );
        assert!(
            matches!(&pool.free_generic_resources[3.into()], GenericResourcePool::Indices(indices) if indices.len() == 1)
        );

        let rq: ResourceRequest = cpus_compact(1).add_generic(3, 2).finish();
        assert!(pool.try_allocate_resources(&rq, None).is_none());

        pool.release_allocation(al);

        assert!(
            matches!(&pool.free_generic_resources[0.into()], GenericResourcePool::Indices(indices) if indices.len() == 96)
        );
        assert!(matches!(
            pool.free_generic_resources[1.into()],
            GenericResourcePool::Sum(100_000_000)
        ));
        assert!(
            matches!(&pool.free_generic_resources[2.into()], GenericResourcePool::Indices(indices) if indices.len() == 2)
        );
        assert!(
            matches!(&pool.free_generic_resources[3.into()], GenericResourcePool::Indices(indices) if indices.len() == 2)
        );
    }
}
