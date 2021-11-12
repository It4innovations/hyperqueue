pub mod allocation;
pub mod descriptor;
pub mod request;

use crate::define_id_type;
pub use allocation::{
    GenericResourceAllocation, GenericResourceAllocationValue, GenericResourceAllocations,
    ResourceAllocation,
};
pub use descriptor::{
    CpusDescriptor, GenericResourceDescriptor, GenericResourceDescriptorKind, ResourceDescriptor,
};
pub use request::{CpuRequest, GenericResourceRequest, ResourceRequest, TimeRequest};

pub type NumOfCpus = u32;
pub type CpuId = u32;

// Identifies a globally unique Resource request stored in Core.
define_id_type!(GenericResourceId, u32);

/// Represents some amount within a single generic resource (e.g. 100 MiB of memory).
pub type GenericResourceAmount = u64;

// Represents an index within a single generic resource (e.g. GPU with ID 1).
define_id_type!(GenericResourceIndex, u32);
