use crate::internal::common::resources::amount::FRACTIONS_PER_UNIT;
use crate::internal::common::resources::{ResourceId, ResourceVec};
use crate::resources::{
    Allocation, ResourceAllocation, ResourceAmount, ResourceFractions, ResourceIndex, ResourceUnits,
};
use crate::Map;
use smallvec::SmallVec;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ConciseResourceState {
    free_units: SmallVec<[ResourceUnits; 1]>,
    fractions: Map<ResourceIndex, ResourceFractions>,
}

impl ConciseResourceState {
    pub fn new(
        free_units: SmallVec<[ResourceUnits; 1]>,
        fractions: Map<ResourceIndex, ResourceFractions>,
    ) -> Self {
        ConciseResourceState {
            free_units,
            fractions,
        }
    }

    fn remove_fractions(
        &mut self,
        group_id: usize,
        resource_idx: ResourceIndex,
        fractions: ResourceFractions,
    ) {
        let old_f = self.fractions.entry(resource_idx).or_insert(0);
        if *old_f < fractions {
            *old_f = FRACTIONS_PER_UNIT + *old_f - fractions;
            assert!(self.free_units[group_id] > 0);
            self.free_units[group_id] -= 1;
        } else {
            *old_f -= fractions;
        }
    }

    pub fn remove(&mut self, resource_allocation: &ResourceAllocation) {
        if self.free_units.len() == 1 {
            let (units, fractions) = resource_allocation.amount.split();
            assert!(self.free_units[0] >= units);
            self.free_units[0] -= units;
            if fractions > 0 {
                if resource_allocation.indices.is_empty() {
                    self.remove_fractions(0, ResourceIndex::new(0), fractions);
                } else {
                    for idx in resource_allocation.indices.iter().rev() {
                        if idx.fractions == 0 {
                            break;
                        }
                        self.remove_fractions(0, idx.index, idx.fractions);
                    }
                }
            }
        } else {
            for idx in &resource_allocation.indices {
                if idx.fractions == 0 {
                    assert!(self.free_units[idx.group_idx as usize] > 0);
                    self.free_units[idx.group_idx as usize] -= 1
                } else {
                    self.remove_fractions(idx.group_idx as usize, idx.index, idx.fractions);
                }
            }
        }
    }

    fn add_fractions(
        &mut self,
        group_id: usize,
        resource_idx: ResourceIndex,
        fractions: ResourceFractions,
    ) {
        let old_f = self.fractions.entry(resource_idx).or_insert(0);
        *old_f += fractions;
        if *old_f >= FRACTIONS_PER_UNIT {
            *old_f -= FRACTIONS_PER_UNIT;
            assert!(*old_f < FRACTIONS_PER_UNIT);
            self.free_units[group_id] += 1;
        }
    }

    pub fn add(&mut self, resource_allocation: &ResourceAllocation) {
        if self.free_units.len() == 1 {
            let (units, fractions) = resource_allocation.amount.split();
            self.free_units[0] += units;
            if fractions > 0 {
                if resource_allocation.indices.is_empty() {
                    self.add_fractions(0, ResourceIndex::new(0), fractions);
                } else {
                    for idx in resource_allocation.indices.iter().rev() {
                        if idx.fractions == 0 {
                            break;
                        }
                        self.add_fractions(0, idx.index, idx.fractions);
                    }
                }
            }
        } else {
            for idx in &resource_allocation.indices {
                if idx.fractions == 0 {
                    self.free_units[idx.group_idx as usize] += 1
                } else {
                    self.add_fractions(idx.group_idx as usize, idx.index, idx.fractions);
                }
            }
        }
    }

    pub fn n_groups(&self) -> usize {
        self.free_units.len()
    }

    pub fn groups(&self) -> &[ResourceUnits] {
        &self.free_units
    }

    pub fn amount_max_alloc(&self) -> ResourceAmount {
        let units = self.free_units.iter().sum();
        let fractions = *self.fractions.values().max().unwrap_or(&0);
        ResourceAmount::new(units, fractions)
    }

    #[cfg(test)]
    pub fn amount_sum(&self) -> ResourceAmount {
        let units = ResourceAmount::new_units(self.free_units.iter().sum());
        let fractions = self
            .fractions
            .values()
            .map(|f| ResourceAmount::new_fractions(*f))
            .sum();
        return units + fractions;
    }

    pub(crate) fn strip_zeros(&self) -> ConciseResourceState {
        ConciseResourceState::new(
            self.free_units.clone(),
            self.fractions
                .iter()
                .filter_map(|(k, v)| if *v > 0 { Some((*k, *v)) } else { None })
                .collect(),
        )
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub(crate) struct ConciseFreeResources {
    resources: ResourceVec<ConciseResourceState>,
}

impl ConciseFreeResources {
    pub fn new(resources: ResourceVec<ConciseResourceState>) -> Self {
        ConciseFreeResources { resources }
    }

    pub fn add(&mut self, allocation: &Allocation) {
        for ra in &allocation.resources {
            self.resources[ra.resource_id].add(ra);
        }
    }

    pub fn remove(&mut self, allocation: &Allocation) {
        for ra in &allocation.resources {
            self.resources[ra.resource_id].remove(ra);
        }
    }

    pub fn n_resources(&self) -> usize {
        self.resources.len()
    }

    pub fn get(&self, resource_id: ResourceId) -> &ConciseResourceState {
        &self.resources[resource_id]
    }

    /*pub fn get_mut(&mut self, resource_id: ResourceId) -> &mut ConciseResourceState {
        &mut self.resources[resource_id]
    }*/

    pub fn all_states(&self) -> &[ConciseResourceState] {
        &self.resources
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
    use crate::resources::ResourceUnits;
    use crate::Map;

    impl ConciseFreeResources {
        pub fn new_simple(counts: &[ResourceUnits]) -> Self {
            Self::new(
                counts
                    .iter()
                    .map(|c| ConciseResourceState::new_simple(&[*c]))
                    .collect::<Vec<_>>()
                    .into(),
            )
        }

        pub fn assert_eq(&self, counts: &[ResourceUnits]) {
            assert_eq!(self, &Self::new_simple(counts));
        }
    }

    impl ConciseResourceState {
        pub fn new_simple(free_units: &[ResourceUnits]) -> Self {
            ConciseResourceState::new(free_units.into(), Map::new())
        }
    }
}
