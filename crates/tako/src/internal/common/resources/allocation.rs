use crate::internal::common::resources::{ResourceAmount, ResourceId, ResourceIndex};
use crate::resources::ResourceFractions;
use smallvec::SmallVec;

#[derive(Debug, Clone)]
pub struct AllocationIndex {
    pub index: ResourceIndex,
    pub group_idx: u32,
    pub fractions: ResourceFractions,
}

#[derive(Debug)]
pub struct ResourceAllocation {
    pub resource_id: ResourceId,
    pub amount: ResourceAmount,
    // INVARIANT: indices are sorted by .fractions, i.e. non-whole allocations are at the end
    pub indices: SmallVec<[AllocationIndex; 1]>,
}

impl ResourceAllocation {
    pub fn resource_indices(&self) -> impl Iterator<Item = ResourceIndex> + '_ {
        self.indices.iter().map(|x| x.index)
    }
}

pub type ResourceAllocations = Vec<ResourceAllocation>;

#[derive(Debug)]
pub struct Allocation {
    pub nodes: Vec<String>,
    pub resources: ResourceAllocations,
}

impl Default for Allocation {
    fn default() -> Self {
        Self::new()
    }
}

impl Allocation {
    pub fn new() -> Self {
        Allocation {
            nodes: Vec::new(),
            resources: Vec::new(),
        }
    }

    pub fn add_resource_allocation(&mut self, ra: ResourceAllocation) {
        self.resources.push(ra);
    }

    pub fn resource_allocation(&self, id: ResourceId) -> Option<&ResourceAllocation> {
        self.resources.iter().find(|r| r.resource_id == id)
    }

    pub fn normalize_allocation(&mut self) {
        self.resources.sort_unstable_by_key(|r| r.resource_id);
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::ResourceId;
    use crate::internal::common::resources::allocation::AllocationIndex;
    use crate::resources::{
        Allocation, ResourceAllocation, ResourceAmount, ResourceIndex, ResourceUnits,
    };

    impl Allocation {
        pub fn new_simple(counts: &[ResourceUnits]) -> Self {
            Allocation {
                nodes: Vec::new(),
                resources: counts
                    .iter()
                    .enumerate()
                    .filter(|(_id, c)| **c > 0)
                    .map(|(id, c)| ResourceAllocation {
                        resource_id: ResourceId::new(id as u32),
                        amount: ResourceAmount::new_units(*c),
                        indices: (0..*c)
                            .map(|x| AllocationIndex {
                                index: ResourceIndex::new(x),
                                group_idx: 0,
                                fractions: 0,
                            })
                            .collect(),
                    })
                    .collect(),
            }
        }
        pub fn get_indices(&self, idx: u32) -> Vec<ResourceIndex> {
            let a = self
                .resources
                .iter()
                .find(|r| r.resource_id == idx.into())
                .unwrap();
            a.indices.iter().map(|a| a.index).collect()
        }
        pub fn get_groups(&self, idx: u32) -> crate::Map<u32, usize> {
            let a = self
                .resources
                .iter()
                .find(|r| r.resource_id == idx.into())
                .unwrap();
            let mut r = crate::Map::new();
            a.indices
                .iter()
                .for_each(|a| *r.entry(a.group_idx).or_default() += 1);
            r
        }
    }
}
