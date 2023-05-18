use crate::format_comma_delimited;
use crate::internal::common::resources::{
    ResourceAmount, ResourceId, ResourceIndex, ResourceUnits,
};
use crate::resources::AllocationRequest::All;
use crate::resources::ResourceFractions;
use smallvec::SmallVec;

/*
#[derive(Debug)]
pub enum AllocationValue {
    Indices(SmallVec<[ResourceIndex; 2]>),
    Sum(ResourceAmount),
}

impl AllocationValue {
    pub fn new_indices(mut indices: SmallVec<[ResourceIndex; 2]>) -> Self {
        indices.sort_unstable();
        AllocationValue::Indices(indices)
    }

    pub fn new_sum(size: ResourceAmount) -> Self {
        AllocationValue::Sum(size)
    }

    pub fn indices(&self) -> Option<&[ResourceIndex]> {
        match self {
            AllocationValue::Indices(indices) => Some(indices),
            AllocationValue::Sum(_) => None,
        }
    }

    pub fn to_comma_delimited_list(&self) -> Option<String> {
        self.indices().map(format_comma_delimited)
    }

    pub fn amount(&self) -> ResourceUnits {
        todo!() // remember fractions for indices
                /*match self {
                    AllocationValue::Indices(indices) => indices.len() as ResourceUnits,
                    AllocationValue::Sum(size) => *size,
                }*/
    }
}*/

#[derive(Debug)]
pub struct AllocationIndex {
    pub index: ResourceIndex,
    pub group_idx: u32,
    pub fractions: ResourceFractions,
}

#[derive(Debug)]
pub struct ResourceAllocation {
    pub resource_id: ResourceId,
    pub amount: ResourceAmount,
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

impl Allocation {
    /*pub fn new_multi_node(nodes: Vec<String>) -> Self {
        Allocation {
            nodes,
            resources: Vec::new(),
        }
    }*/

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
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::allocation::AllocationIndex;
    use crate::internal::common::resources::ResourceId;
    use crate::resources::{
        Allocation, ResourceAllocation, ResourceAmount, ResourceIndex, ResourceUnits,
    };
    use crate::Set;

    impl Allocation {
        pub fn new_simple(counts: &[ResourceUnits]) -> Self {
            Allocation {
                nodes: Vec::new(),
                resources: counts
                    .iter()
                    .enumerate()
                    .filter(|(id, c)| **c > 0)
                    .map(|(id, c)| ResourceAllocation {
                        resource_id: ResourceId::new(id as u32),
                        amount: ResourceAmount::new_units(*c),
                        indices: (0..*c)
                            .map(|x| AllocationIndex {
                                index: ResourceIndex::new(x as u32),
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
    }

    /*
    impl AllocationValue {
        pub fn get_checked_indices(&self) -> Vec<u32> {
            match self {
                AllocationValue::Indices(indices) => {
                    let v: Vec<u32> = indices.iter().map(|x| x.as_num()).collect();
                    assert_eq!(v.iter().collect::<Set<_>>().len(), v.len());
                    v
                }
                AllocationValue::Sum(_) => {
                    panic!("Not indices")
                }
            }
        }
    }*/
}
