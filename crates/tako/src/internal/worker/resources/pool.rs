use crate::internal::common::resources::allocation::AllocationIndex;
use crate::internal::common::resources::amount::FRACTIONS_PER_UNIT;
use crate::internal::common::resources::descriptor::ResourceDescriptorKind;
use crate::internal::common::resources::{ResourceAmount, ResourceId, ResourceIndex};
use crate::internal::worker::resources::concise::{ConciseResourceGroup, ConciseResourceState};
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::resources::{AllocationRequest, ResourceAllocation, ResourceFractions, ResourceUnits};
use crate::{Map, Set};

use crate::internal::worker::resources::groups::find_compact_groups;
use smallvec::{SmallVec, smallvec};

#[derive(Debug)]
pub(crate) struct IndicesResourcePool {
    full_size: ResourceAmount,
    indices: Vec<ResourceIndex>,
    fractions: Map<ResourceIndex, ResourceFractions>,
}

#[derive(Debug)]
pub(crate) struct GroupsResourcePool {
    full_size: ResourceAmount,
    indices: Vec<Vec<ResourceIndex>>,
    fractions: Vec<Map<ResourceIndex, ResourceFractions>>,
    min_group_size: ResourceAmount,
    is_coupled: bool,
    //reverse_map: Map<ResourceIndex, usize>,
}

impl GroupsResourcePool {
    pub fn group_amounts(&self) -> impl Iterator<Item = ResourceAmount> {
        self.indices
            .iter()
            .zip(self.fractions.iter())
            .map(|(i, f)| {
                ResourceAmount::new(
                    i.len() as ResourceUnits,
                    f.values().max().copied().unwrap_or(0),
                )
            })
    }

    pub fn is_coupled(&self) -> bool {
        self.is_coupled
    }

    pub fn n_groups(&self) -> usize {
        self.indices.len()
    }
}

#[derive(Debug)]
pub(crate) struct SumResourcePool {
    full_size: ResourceAmount,
    free: ResourceAmount,
}

#[derive(Debug)]
pub(crate) enum ResourcePool {
    Empty,
    Indices(IndicesResourcePool),
    Groups(GroupsResourcePool),
    Sum(SumResourcePool),
}

/// This is a constant used to determine the maximal reasonable number of groups
/// It is used size for SmallVecs in the allocation process. Therefore, the whole
/// system will work even if this number is higher, but then it will allocate memory.
pub const FAST_MAX_GROUPS: usize = 8;
pub const FAST_MAX_COUPLED_RESOURCES: usize = 3;

impl ResourcePool {
    pub fn new(
        kind: &ResourceDescriptorKind,
        resource_id: ResourceId,
        label_map: &ResourceLabelMap,
        is_coupled: bool,
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
                full_size: ResourceAmount::new_units(values.len() as ResourceUnits),
                fractions: Map::new(),
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
                    full_size: ResourceAmount::new_units(
                        groups.iter().map(|g| g.len() as ResourceUnits).sum(),
                    ),
                    min_group_size: ResourceAmount::new_units(
                        groups
                            .iter()
                            .map(|g| g.len() as ResourceUnits)
                            .min()
                            .unwrap_or(1),
                    ),
                    fractions: groups.iter().map(|_| Map::new()).collect(),
                    is_coupled,
                })
            }
            ResourceDescriptorKind::Range { start, end } => {
                let indices: Vec<ResourceIndex> = (start.as_num()..=end.as_num())
                    .map(|id| id.into())
                    .collect();
                ResourcePool::Indices(IndicesResourcePool {
                    full_size: ResourceAmount::new_units(indices.len() as ResourceUnits),
                    indices,
                    fractions: Map::new(),
                })
            }
            ResourceDescriptorKind::Sum { size } => ResourcePool::Sum(SumResourcePool {
                full_size: *size,
                free: *size,
            }),
        }
    }

    pub fn as_groups_mut(&mut self) -> &mut GroupsResourcePool {
        match self {
            ResourcePool::Groups(g) => g,
            _ => unreachable!(),
        }
    }

    pub fn full_size(&self) -> ResourceAmount {
        match self {
            ResourcePool::Empty => ResourceAmount::ZERO,
            ResourcePool::Indices(pool) => pool.full_size,
            ResourcePool::Groups(pool) => pool.full_size,
            ResourcePool::Sum(pool) => pool.full_size,
        }
    }

    pub fn min_group_size(&self) -> ResourceUnits {
        match self {
            ResourcePool::Empty => 0,
            ResourcePool::Indices(pool) => pool.full_size.units(),
            ResourcePool::Groups(pool) => pool.min_group_size.units(),
            ResourcePool::Sum(pool) => pool.full_size.units(),
        }
    }

    pub fn n_groups(&self) -> usize {
        match self {
            ResourcePool::Empty | ResourcePool::Indices(..) | ResourcePool::Sum(..) => 1,
            ResourcePool::Groups(pool) => pool.n_groups(),
        }
    }

    pub(crate) fn concise_state(&self) -> ConciseResourceState {
        let free = match self {
            ResourcePool::Empty => smallvec![],
            ResourcePool::Indices(pool) => {
                smallvec![ConciseResourceGroup::new(
                    pool.indices.len() as ResourceUnits,
                    pool.fractions.clone()
                )]
            }
            ResourcePool::Sum(pool) => {
                let mut fractions: Map<ResourceIndex, ResourceFractions> = Map::new();
                let (units, frac) = pool.free.split();
                if frac > 0 {
                    fractions.insert(ResourceIndex::new(0), frac);
                }
                smallvec![ConciseResourceGroup::new(units, fractions)]
            }
            ResourcePool::Groups(pool) => pool
                .indices
                .iter()
                .zip(pool.fractions.iter())
                .map(|(indices, fractions)| {
                    ConciseResourceGroup::new(indices.len() as ResourceUnits, fractions.clone())
                })
                .collect(),
        };
        ConciseResourceState::new(free)
    }

    fn claim_all_from_groups(pool: &mut GroupsResourcePool) -> SmallVec<[AllocationIndex; 1]> {
        pool.indices
            .iter_mut()
            .enumerate()
            .flat_map(|(group_id, group)| {
                std::mem::take(group)
                    .into_iter()
                    .map(move |index| AllocationIndex {
                        index,
                        group_idx: group_id as u32,
                        fractions: 0,
                    })
            })
            .collect()
    }

    fn claim_scatter_from_groups(
        amount: ResourceAmount,
        pool: &mut GroupsResourcePool,
        group_set: Option<&[usize]>,
    ) -> SmallVec<[AllocationIndex; 1]> {
        let mut indices: SmallVec<[AllocationIndex; 1]> = Default::default();
        let (mut units, mut fractions) = amount.split();

        let mut index = 0;
        while units > 0 || fractions > 0 {
            let group_idx = if let Some(gs) = group_set {
                gs[index]
            } else {
                index
            };
            if units > 0 {
                if let Some(index) = pool.indices[group_idx].pop() {
                    units -= 1;
                    indices.push(AllocationIndex {
                        index,
                        group_idx: group_idx as u32,
                        fractions: 0,
                    })
                }
            } else if let Some((index, f)) =
                Self::best_fraction_match(&mut pool.fractions[group_idx], fractions)
            {
                *f -= fractions;
                indices.push(AllocationIndex {
                    index: *index,
                    group_idx: group_idx as u32,
                    fractions,
                });
                fractions = 0;
            } else if let Some(index) = pool.indices[group_idx].pop() {
                pool.fractions[group_idx].insert(index, FRACTIONS_PER_UNIT - fractions);
                indices.push(AllocationIndex {
                    index,
                    group_idx: group_idx as u32,
                    fractions,
                });
                fractions = 0;
            }
            index += 1;
            if let Some(gs) = group_set {
                index %= gs.len();
            } else {
                index %= pool.indices.len();
            };
        }
        indices.sort_by_key(|i| (i.fractions, i.group_idx, i.index));
        indices
    }

    fn claim_compact_from_groups(
        amount: ResourceAmount,
        pool: &mut GroupsResourcePool,
        group_set: Option<&[usize]>,
    ) -> SmallVec<[AllocationIndex; 1]> {
        let mut indices = Default::default();
        let mut remaining = amount;
        let mut fraction_idx: Option<usize> = None;

        let mut amounts: SmallVec<[ResourceAmount; FAST_MAX_GROUPS]> =
            pool.group_amounts().collect();
        loop {
            if let Some((group_idx, _a)) = amounts
                .iter()
                .enumerate()
                .filter(|(i, a)| **a >= remaining && group_set.is_none_or(|gs| gs.contains(i)))
                .min_by_key(|(_i, a)| **a)
            {
                let (units, fractions) = remaining.split();
                Self::take_indices(
                    &mut pool.indices[group_idx],
                    group_idx as u32,
                    units,
                    &mut indices,
                );
                Self::take_fraction_index_or_split(
                    &mut pool.fractions[group_idx],
                    &mut pool.indices[group_idx],
                    fractions,
                    group_idx as u32,
                    &mut indices,
                );
                break;
            } else {
                let (group_idx, amount) = amounts
                    .iter_mut()
                    .enumerate()
                    .filter(|(i, _)| group_set.is_none_or(|gs| gs.contains(i)))
                    .max_by_key(|(_i, a)| **a)
                    .unwrap();
                *amount = ResourceAmount::ZERO;
                let (mut units, mut fractions) = remaining.split();
                let size = pool.indices[group_idx].len() as ResourceUnits;
                units -= size;
                Self::take_indices(
                    &mut pool.indices[group_idx],
                    group_idx as u32,
                    size,
                    &mut indices,
                );
                if Self::try_take_fraction(
                    &mut pool.fractions[group_idx],
                    fractions,
                    group_idx as u32,
                    &mut indices,
                ) {
                    fraction_idx = Some(indices.len() - 1);
                    fractions = 0;
                }
                remaining = ResourceAmount::new(units, fractions)
            }
        }

        if let Some(allocation_idx) = fraction_idx {
            let last = indices.len() - 1;
            indices.swap(allocation_idx, last);
        }

        indices
    }

    pub fn take_indices(
        pool_indices: &mut Vec<ResourceIndex>,
        group_id: u32,
        units: ResourceUnits,
        out: &mut SmallVec<[AllocationIndex; 1]>,
    ) {
        (0..units).for_each(|_| {
            out.push(AllocationIndex {
                index: pool_indices.pop().unwrap(),
                group_idx: group_id,
                fractions: 0,
            })
        })
    }

    fn take_fraction_index_or_split(
        pool_fractions: &mut Map<ResourceIndex, ResourceFractions>,
        pool_indices: &mut Vec<ResourceIndex>,
        fractions: ResourceFractions,
        group_idx: u32,
        out: &mut SmallVec<[AllocationIndex; 1]>,
    ) {
        if fractions > 0 {
            out.push(
                if let Some((index, f)) = Self::best_fraction_match(pool_fractions, fractions) {
                    *f -= fractions;
                    AllocationIndex {
                        index: *index,
                        group_idx,
                        fractions,
                    }
                } else {
                    let index = pool_indices.pop().unwrap();
                    pool_fractions.insert(index, FRACTIONS_PER_UNIT - fractions);
                    AllocationIndex {
                        index,
                        group_idx,
                        fractions,
                    }
                },
            )
        }
    }

    fn try_take_fraction(
        pool_fractions: &mut Map<ResourceIndex, ResourceFractions>,
        fractions: ResourceFractions,
        group_idx: u32,
        out: &mut SmallVec<[AllocationIndex; 1]>,
    ) -> bool {
        if fractions > 0 {
            if let Some((index, f)) = Self::best_fraction_match(pool_fractions, fractions) {
                *f -= fractions;
                out.push(AllocationIndex {
                    index: *index,
                    group_idx,
                    fractions,
                });
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn best_fraction_match(
        frac_map: &mut Map<ResourceIndex, ResourceFractions>,
        fractions: ResourceFractions,
    ) -> Option<(&ResourceIndex, &mut ResourceFractions)> {
        frac_map
            .iter_mut()
            .filter(|(_, f)| **f >= fractions)
            .min_by_key(|(_, f)| **f)
    }

    pub(crate) fn claim_resources_with_group_mask(
        &mut self,
        resource_id: ResourceId,
        policy: &AllocationRequest,
        group_set: &[usize],
    ) -> ResourceAllocation {
        let pool = self.as_groups_mut();
        let (amount, indices) = match policy {
            AllocationRequest::Compact(amount) | AllocationRequest::ForceCompact(amount) => (
                *amount,
                Self::claim_scatter_from_groups(*amount, pool, Some(group_set)),
            ),
            AllocationRequest::Tight(amount) | AllocationRequest::ForceTight(amount) => (
                *amount,
                Self::claim_compact_from_groups(*amount, pool, Some(group_set)),
            ),
            AllocationRequest::Scatter(_) | AllocationRequest::All => unreachable!(),
        };
        ResourceAllocation {
            resource_id,
            amount,
            indices,
        }
    }

    pub(crate) fn claim_resources(
        &mut self,
        resource_id: ResourceId,
        free: &ConciseResourceState,
        policy: &AllocationRequest,
    ) -> ResourceAllocation {
        let (amount, indices) = match self {
            ResourcePool::Empty => unreachable!(),
            ResourcePool::Indices(pool) => {
                let amount = policy.amount(pool.full_size);
                let (units, fractions) = amount.split();
                let mut indices = Default::default();
                Self::take_indices(&mut pool.indices, 0, units, &mut indices);
                Self::take_fraction_index_or_split(
                    &mut pool.fractions,
                    &mut pool.indices,
                    fractions,
                    0,
                    &mut indices,
                );
                (amount, indices)
            }
            ResourcePool::Groups(pool) => {
                match policy {
                    AllocationRequest::Compact(amount)
                    | AllocationRequest::ForceCompact(amount) => {
                        // We do not need to distinguish between force compact and compact
                        // because we already know that allocation is possible
                        let group_set = find_compact_groups(pool.n_groups(), free, policy).unwrap();
                        (
                            *amount,
                            Self::claim_scatter_from_groups(*amount, pool, Some(&group_set)),
                        )
                    }
                    AllocationRequest::Tight(amount) | AllocationRequest::ForceTight(amount) => {
                        // We do neet need to distinguish between force tight and tight
                        // because we already know that here that allocation is possible
                        let group_set = find_compact_groups(pool.n_groups(), free, policy).unwrap();
                        (
                            *amount,
                            Self::claim_compact_from_groups(*amount, pool, Some(&group_set)),
                        )
                    }
                    AllocationRequest::Scatter(amount) => (
                        *amount,
                        Self::claim_scatter_from_groups(*amount, pool, None),
                    ),
                    AllocationRequest::All => (pool.full_size, Self::claim_all_from_groups(pool)),
                }
            }
            ResourcePool::Sum(pool) => {
                let amount = policy.amount(pool.full_size);
                pool.free -= amount;
                (amount, Default::default())
            }
        };
        ResourceAllocation {
            resource_id,
            amount,
            indices,
        }
    }

    pub fn release_allocation(&mut self, allocation: &ResourceAllocation) {
        match self {
            ResourcePool::Empty => unreachable!(),
            ResourcePool::Indices(pool) => {
                for index in allocation.indices.iter().rev() {
                    // Iterating reversely, as indices were originally taken by pop()
                    // Just to add small determinism, we return then in the same order
                    assert_eq!(index.group_idx, 0);
                    if index.fractions == 0 {
                        pool.indices.push(index.index);
                    } else {
                        let f = pool.fractions.get_mut(&index.index).unwrap();
                        *f += index.fractions;
                        if *f == FRACTIONS_PER_UNIT {
                            pool.fractions.remove(&index.index);
                            pool.indices.push(index.index);
                        }
                    }
                }
                //pool.indices.extend(&allocation.resource_indices())
            }
            ResourcePool::Sum(pool) => {
                pool.free += allocation.amount;
                assert!(pool.free <= pool.full_size);
                assert!(allocation.indices.is_empty());
            }
            ResourcePool::Groups(pool) => {
                for index in allocation.indices.iter().rev() {
                    // Iterating reversely, as indices were originally taken by pop()
                    // Just to add small determinism, we return then in the same order
                    if index.fractions == 0 {
                        pool.indices[index.group_idx as usize].push(index.index);
                    } else {
                        let f = pool.fractions[index.group_idx as usize]
                            .get_mut(&index.index)
                            .unwrap();
                        *f += index.fractions;
                        if *f == FRACTIONS_PER_UNIT {
                            pool.fractions[index.group_idx as usize].remove(&index.index);
                            pool.indices[index.group_idx as usize].push(index.index);
                        }
                    }
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
                assert!(pool.indices.len() <= pool.full_size.units() as usize);
                for index in &pool.indices {
                    assert!(!pool.fractions.contains_key(index));
                }
                for f in pool.fractions.values() {
                    //assert!(*f > 0);
                    assert!(*f < FRACTIONS_PER_UNIT);
                }
            }
            ResourcePool::Groups(pool) => {
                let sum: usize = pool.indices.iter().map(|x| x.len()).sum();
                assert_eq!(Set::from_iter(pool.indices.iter().flatten()).len(), sum);
                assert!(sum <= pool.full_size.units() as usize);
                for index in pool.indices.iter().flatten() {
                    for f in &pool.fractions {
                        assert!(!f.contains_key(index));
                    }
                }
                for f in &pool.fractions {
                    for ff in f.values() {
                        assert!(*ff < FRACTIONS_PER_UNIT);
                    }
                }
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
    use crate::resources::{ResourceAmount, ResourceUnits};

    impl ResourcePool {
        pub fn current_free(&self) -> ResourceAmount {
            match self {
                ResourcePool::Empty => ResourceAmount::ZERO,
                ResourcePool::Indices(pool) => {
                    ResourceAmount::new_units(pool.indices.len() as ResourceUnits)
                }
                ResourcePool::Groups(pool) => ResourceAmount::new_units(
                    pool.indices.iter().map(|g| g.len()).sum::<usize>() as ResourceUnits,
                ),
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
                ResourcePool::Groups(_pool) => {
                    let mut result: Vec<_> = allocation
                        .indices
                        .iter()
                        .map(|idx| idx.group_idx as usize)
                        .collect();
                    result.sort();
                    result.dedup();
                    result
                }
            }
        }
    }
}
