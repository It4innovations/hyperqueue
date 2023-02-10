use crate::internal::common::resources::descriptor::ResourceDescriptorKind;
use crate::internal::common::resources::{
    AllocationValue, ResourceAmount, ResourceId, ResourceIndex,
};
use crate::internal::common::Map;
use crate::internal::worker::resources::counts::ResourceCount;
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::Set;
use smallvec::smallvec;

#[derive(Debug)]
pub struct IndicesResourcePool {
    full_size: ResourceAmount,
    indices: Vec<ResourceIndex>,
}

#[derive(Debug)]
pub struct GroupsResourcePool {
    full_size: ResourceAmount,
    indices: Vec<Vec<ResourceIndex>>,
    min_group_size: ResourceAmount,
    reverse_map: Map<ResourceIndex, usize>,
}

#[derive(Debug)]
pub struct SumResourcePool {
    full_size: ResourceAmount,
    free: ResourceAmount,
}

#[derive(Debug)]
pub enum ResourcePool {
    Empty,
    Indices(IndicesResourcePool),
    Groups(GroupsResourcePool),
    Sum(SumResourcePool),
}

impl ResourcePool {
    pub fn new(
        kind: &ResourceDescriptorKind,
        resource_id: ResourceId,
        label_map: &ResourceLabelMap,
    ) -> Self {
        match kind {
            ResourceDescriptorKind::List { values } => ResourcePool::Indices(IndicesResourcePool {
                indices: values
                    .iter()
                    .map(|label| {
                        label_map
                            .get_index(resource_id, label)
                            .expect("Resource label not found")
                    })
                    .collect(),
                full_size: values.len() as ResourceAmount,
            }),
            ResourceDescriptorKind::Groups { groups } => {
                let groups: Vec<Vec<ResourceIndex>> = groups
                    .iter()
                    .map(|labels| {
                        labels
                            .iter()
                            .map(|label| {
                                label_map
                                    .get_index(resource_id, label)
                                    .expect("Resource label not found")
                            })
                            .collect()
                    })
                    .collect();

                ResourcePool::Groups(GroupsResourcePool {
                    indices: groups.clone(),
                    full_size: groups.iter().map(|g| g.len() as ResourceAmount).sum(),
                    reverse_map: groups
                        .iter()
                        .enumerate()
                        .flat_map(|(i, group)| group.iter().map(move |idx| (*idx, i)))
                        .collect(),
                    min_group_size: groups
                        .iter()
                        .map(|g| g.len() as ResourceAmount)
                        .min()
                        .unwrap_or(1),
                })
            }
            ResourceDescriptorKind::Range { start, end } => {
                let indices: Vec<ResourceIndex> = (start.as_num()..=end.as_num())
                    .map(|id| id.into())
                    .collect();
                ResourcePool::Indices(IndicesResourcePool {
                    full_size: indices.len() as ResourceAmount,
                    indices,
                })
            }
            ResourceDescriptorKind::Sum { size } => ResourcePool::Sum(SumResourcePool {
                full_size: *size,
                free: *size,
            }),
        }
    }

    pub fn full_size(&self) -> ResourceAmount {
        match self {
            ResourcePool::Empty => 0,
            ResourcePool::Indices(pool) => pool.full_size,
            ResourcePool::Groups(pool) => pool.full_size,
            ResourcePool::Sum(pool) => pool.full_size,
        }
    }

    pub fn min_group_size(&self) -> ResourceAmount {
        match self {
            ResourcePool::Empty => 0,
            ResourcePool::Indices(pool) => pool.full_size,
            ResourcePool::Groups(pool) => pool.min_group_size,
            ResourcePool::Sum(pool) => pool.full_size,
        }
    }

    pub fn n_groups(&self) -> usize {
        match self {
            ResourcePool::Empty | ResourcePool::Indices(..) | ResourcePool::Sum(..) => 1,
            ResourcePool::Groups(pool) => pool.indices.len(),
        }
    }

    pub fn count(&self) -> ResourceCount {
        match self {
            ResourcePool::Empty => smallvec![],
            ResourcePool::Indices(pool) => smallvec![pool.indices.len() as ResourceAmount],
            ResourcePool::Sum(pool) => smallvec![pool.free],
            ResourcePool::Groups(pool) => pool
                .indices
                .iter()
                .map(|g| g.len() as ResourceAmount)
                .collect(),
        }
    }

    pub fn claim_resources(&mut self, amount: &ResourceCount) -> AllocationValue {
        match self {
            ResourcePool::Empty => unreachable!(),
            ResourcePool::Indices(ref mut pool) => {
                assert_eq!(amount.len(), 1);
                AllocationValue::new_indices(
                    (0..amount[0])
                        .map(|_| pool.indices.pop().unwrap())
                        .collect(),
                )
            }
            ResourcePool::Sum(ref mut pool) => {
                assert_eq!(amount.len(), 1);
                assert!(pool.free >= amount[0]);
                pool.free -= amount[0];
                AllocationValue::new_sum(amount[0])
            }
            ResourcePool::Groups(pool) => {
                let mut result = smallvec![];
                assert!(pool.indices.len() >= amount.len());
                for (g, a) in pool.indices.iter_mut().zip(amount) {
                    for _ in 0..*a {
                        result.push(g.pop().unwrap())
                    }
                }
                AllocationValue::new_indices(result)
            }
        }
    }

    pub fn release_allocation(&mut self, allocation: AllocationValue) {
        match self {
            ResourcePool::Empty => unreachable!(),
            ResourcePool::Indices(pool) => pool
                .indices
                .extend_from_slice(allocation.indices().unwrap()),
            ResourcePool::Sum(pool) => match allocation {
                AllocationValue::Sum(amount) => {
                    pool.free += amount;
                    assert!(pool.free <= pool.full_size);
                }
                _ => unreachable!(),
            },
            ResourcePool::Groups(pool) => {
                for idx in allocation.indices().unwrap().iter() {
                    pool.indices[pool.reverse_map[idx]].push(*idx);
                }
            }
        };

        #[cfg(debug_assertions)]
        self.validate()
    }

    #[cfg(debug_assertions)]
    pub fn validate(&self) {
        match self {
            ResourcePool::Empty => {}
            ResourcePool::Indices(pool) => {
                assert_eq!(
                    Set::from_iter(pool.indices.iter()).len(),
                    pool.indices.len()
                );
                assert!(pool.indices.len() <= pool.full_size as usize)
            }
            ResourcePool::Groups(pool) => {
                let sum: usize = pool.indices.iter().map(|x| x.len()).sum();
                assert_eq!(Set::from_iter(pool.indices.iter().flatten()).len(), sum);
                assert!(sum <= pool.full_size as usize)
            }
            ResourcePool::Sum(pool) => {
                assert!(pool.free <= pool.full_size)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::ResourceAllocation;
    use crate::internal::worker::resources::pool::ResourcePool;
    use crate::resources::ResourceAmount;

    impl ResourcePool {
        pub fn current_free(&self) -> ResourceAmount {
            match self {
                ResourcePool::Empty => 0,
                ResourcePool::Indices(pool) => pool.indices.len() as ResourceAmount,
                ResourcePool::Groups(pool) => {
                    pool.indices.iter().map(|g| g.len() as ResourceAmount).sum()
                }
                ResourcePool::Sum(pool) => pool.free,
            }
        }

        pub fn get_sockets(&self, allocation: &ResourceAllocation) -> Vec<usize> {
            match self {
                ResourcePool::Empty => {
                    unreachable!()
                }
                ResourcePool::Sum(_) => {
                    unreachable!()
                }
                ResourcePool::Indices(_) => {
                    vec![0]
                }
                ResourcePool::Groups(pool) => {
                    let mut result: Vec<_> = allocation
                        .value
                        .indices()
                        .unwrap()
                        .iter()
                        .map(|idx| *pool.reverse_map.get(idx).unwrap())
                        .collect();
                    result.sort();
                    result.dedup();
                    result
                }
            }
        }
    }
}
