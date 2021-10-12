pub mod allocation;
pub mod descriptor;
pub mod request;

pub type NumOfCpus = u32;
pub type CpuId = u32;

/// Resource identification wrt. global resource list (stored in Core)
pub type GenericResourceId = u32;
pub type GenericResourceAmount = u64;
pub type GenericResourceIndex = u32;

pub use allocation::{
    GenericResourceAllocation, GenericResourceAllocationValue, GenericResourceAllocations,
    ResourceAllocation,
};
pub use descriptor::{
    CpusDescriptor, GenericResourceDescriptor, GenericResourceDescriptorKind, ResourceDescriptor,
};
pub use request::{CpuRequest, GenericResourceRequest, ResourceRequest, TimeRequest};
