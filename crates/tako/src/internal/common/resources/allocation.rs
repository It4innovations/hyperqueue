use crate::internal::common::resources::CpuId;
use crate::internal::common::resources::{
    GenericResourceAmount, GenericResourceId, GenericResourceIndex,
};
use crate::internal::common::utils::format_comma_delimited;
use smallvec::SmallVec;

#[derive(Debug)]
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
            GenericResourceAllocationValue::Indices(indices) => {
                Some(format_comma_delimited(indices))
            }
            GenericResourceAllocationValue::Sum(_) => None,
        }
    }
}

#[derive(Debug)]
pub struct GenericResourceAllocation {
    pub resource: GenericResourceId,
    pub value: GenericResourceAllocationValue,
}

pub type GenericResourceAllocations = SmallVec<[GenericResourceAllocation; 2]>;

#[derive(Debug)]
pub struct ResourceAllocation {
    pub nodes: Vec<String>,
    pub cpus: Vec<CpuId>,
    pub generic_allocations: GenericResourceAllocations,
}

impl ResourceAllocation {
    #[inline]
    pub fn new(
        nodes: Vec<String>,
        cpus: Vec<CpuId>,
        generic_allocations: GenericResourceAllocations,
    ) -> Self {
        ResourceAllocation {
            nodes,
            cpus,
            generic_allocations,
        }
    }

    pub fn comma_delimited_cpu_ids(&self) -> String {
        format_comma_delimited(&self.cpus)
    }
}
