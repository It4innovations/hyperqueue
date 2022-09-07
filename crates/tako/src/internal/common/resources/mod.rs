pub mod allocation;
pub mod descriptor;
pub mod map;
pub mod request;

use crate::define_id_type;
use crate::internal::common::index::IndexVec;
pub use allocation::{Allocation, AllocationValue, ResourceAllocation, ResourceAllocations};
pub use descriptor::{
    DescriptorError, ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind,
};
pub use map::{CPU_RESOURCE_ID, CPU_RESOURCE_NAME, GPU_RESOURCE_NAME, MEM_RESOURCE_NAME};
pub use request::{
    AllocationRequest, ResourceRequest, ResourceRequestEntries, ResourceRequestEntry, TimeRequest,
};

pub type NumOfNodes = u32;

// Identifies a globally unique Resource request stored in Core.
define_id_type!(ResourceId, u32);

/// Represents some amount within a single generic resource (e.g. 100 MiB of memory).
pub type ResourceAmount = u64;

// Represents an index within a single generic resource (e.g. GPU with ID 1).
define_id_type!(ResourceIndex, u32);

pub type ResourceVec<T> = IndexVec<ResourceId, T>;
