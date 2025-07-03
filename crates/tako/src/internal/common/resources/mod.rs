pub mod allocation;
pub mod amount;
pub mod descriptor;
pub mod map;
pub mod request;

use crate::define_id_type;
use crate::internal::common::index::IndexVec;
pub use allocation::{Allocation, ResourceAllocation};
pub use descriptor::{
    ResourceDescriptiorCoupling, ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind,
};
pub use map::{
    AMD_GPU_RESOURCE_NAME, CPU_RESOURCE_ID, CPU_RESOURCE_NAME, MEM_RESOURCE_NAME,
    NVIDIA_GPU_RESOURCE_NAME,
};
pub use request::{
    AllocationRequest, ResourceAllocRequest, ResourceRequest, ResourceRequestEntries,
    ResourceRequestVariants, TimeRequest,
};

pub use amount::{ResourceAmount, ResourceFractions, ResourceUnits};

pub type NumOfNodes = u32;

// Identifies a globally unique Resource request stored in Core.
define_id_type!(ResourceId, u32);

// Represents an index within a single generic resource (e.g. GPU with ID 1).
define_id_type!(ResourceIndex, u32);

// Represents a label of an individual resource provided by a worker
pub type ResourceLabel = String;

pub type ResourceVec<T> = IndexVec<ResourceId, T>;
