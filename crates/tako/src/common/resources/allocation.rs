use crate::common::resources::CpuId;
use crate::common::resources::{GenericResourceAmount, GenericResourceId, GenericResourceIndex};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum GenericResourceAllocationValue {
    Indices(SmallVec<[GenericResourceIndex; 2]>),
    Sum(GenericResourceAmount),
}

impl GenericResourceAllocationValue {
    pub fn new_indices(mut indices: SmallVec<[GenericResourceIndex; 2]>) -> Self {
        indices.sort_unstable();
        GenericResourceAllocationValue::Indices(indices)
    }

    pub fn new_sum(size: GenericResourceAmount) -> Self {
        GenericResourceAllocationValue::Sum(size)
    }

    pub fn to_comma_delimited_list(&self) -> Option<String> {
        match self {
            GenericResourceAllocationValue::Indices(indices) => Some(
                indices
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            GenericResourceAllocationValue::Sum(_) => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct GenericResourceAllocation {
    pub resource: GenericResourceId,
    pub value: GenericResourceAllocationValue,
}

pub type GenericResourceAllocations = SmallVec<[GenericResourceAllocation; 2]>;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ResourceAllocation {
    pub cpus: Vec<CpuId>,
    pub generic_allocations: GenericResourceAllocations,
}

impl ResourceAllocation {
    pub fn new(cpus: Vec<CpuId>, generic_allocations: GenericResourceAllocations) -> Self {
        ResourceAllocation {
            cpus,
            generic_allocations,
        }
    }

    pub fn comma_delimited_cpu_ids(&self) -> String {
        self.cpus
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}
