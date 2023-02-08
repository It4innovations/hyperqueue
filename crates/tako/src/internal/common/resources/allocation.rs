use crate::format_comma_delimited;
use crate::internal::common::resources::{ResourceAmount, ResourceId, ResourceIndex};
use crate::internal::worker::resources::counts::ResourceCountVec;
use smallvec::SmallVec;

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

    pub fn amount(&self) -> ResourceAmount {
        match self {
            AllocationValue::Indices(indices) => indices.len() as ResourceAmount,
            AllocationValue::Sum(amount) => *amount,
        }
    }
}

#[derive(Debug)]
pub struct ResourceAllocation {
    pub resource: ResourceId,
    pub value: AllocationValue,
}

pub type ResourceAllocations = SmallVec<[ResourceAllocation; 2]>;

#[derive(Debug)]
pub struct Allocation {
    pub nodes: Vec<String>,
    pub resources: ResourceAllocations,
    pub counts: ResourceCountVec, // Store counts profile - it is used when Allocation is freed, to find the right profile
}

impl Allocation {
    #[inline]
    pub fn new(
        nodes: Vec<String>,
        resources: ResourceAllocations,
        counts: ResourceCountVec,
    ) -> Self {
        Allocation {
            nodes,
            resources,
            counts,
        }
    }

    pub fn resource_allocation(&self, id: ResourceId) -> Option<&ResourceAllocation> {
        self.resources.iter().find(|r| r.resource == id)
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::AllocationValue;
    use crate::resources::{Allocation, ResourceAmount, ResourceIndex};
    use crate::Set;

    impl Allocation {
        pub fn get_indices(&self, idx: u32) -> Vec<ResourceIndex> {
            let a = self
                .resources
                .iter()
                .find(|r| r.resource == idx.into())
                .unwrap();
            match &a.value {
                AllocationValue::Indices(x) => x.to_vec(),
                AllocationValue::Sum(_) => panic!("Sum not indices"),
            }
        }

        pub fn get_sum(&self, idx: u32) -> ResourceAmount {
            let a = self
                .resources
                .iter()
                .find(|r| r.resource == idx.into())
                .unwrap();
            match &a.value {
                AllocationValue::Indices(_) => panic!("Indices not sum"),
                AllocationValue::Sum(s) => *s,
            }
        }
    }

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
    }
}
