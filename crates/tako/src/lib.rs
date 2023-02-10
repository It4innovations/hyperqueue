#![deny(clippy::await_holding_refcell_ref)]

/*#[cfg(not(bench))]
#[macro_use]
pub(crate) mod internal;*/

#[macro_use]
pub mod internal;

pub mod comm;
pub mod gateway;
pub mod hwstats;
pub mod launcher;
pub mod program;

pub use crate::internal::common::index::{AsIdVec, ItemId};
pub use crate::internal::common::taskgroup::TaskGroup;
pub use crate::internal::common::utils::format_comma_delimited;
pub use crate::internal::common::WrappedRcRefCell;
pub use crate::internal::common::{Map, Set};

define_id_type!(WorkerId, u32);
define_id_type!(TaskId, u32);
define_id_type!(InstanceId, u32);

// Priority: Bigger number -> Higher priority
pub type Priority = i32;
pub type PriorityTuple = (Priority, Priority); // user priority, scheduler priority

pub type Error = internal::common::error::DsError;
pub type Result<T> = std::result::Result<T, Error>;

pub mod resources {
    pub use crate::internal::common::resources::{
        Allocation, AllocationRequest, AllocationValue, NumOfNodes, ResourceAmount,
        ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind, ResourceIndex,
        ResourceLabel, ResourceRequest, ResourceRequestEntries, ResourceRequestEntry, TimeRequest,
        CPU_RESOURCE_ID, CPU_RESOURCE_NAME, GPU_RESOURCE_NAME, MEM_RESOURCE_NAME,
    };

    pub use crate::internal::common::resources::map::ResourceMap;

    pub use crate::internal::common::resources::descriptor::DescriptorError;
}

pub mod server {
    pub use crate::internal::server::rpc::ConnectionDescriptor;
    pub use crate::internal::server::start::server_start;
}

pub mod worker {
    pub use crate::internal::messages::worker::WorkerOverview;
    pub use crate::internal::worker::configuration::ServerLostPolicy;
    pub use crate::internal::worker::configuration::WorkerConfiguration;

    pub use crate::internal::worker::rpc::run_worker;
}

pub mod task {
    pub type SerializedTaskContext = Vec<u8>;
}
