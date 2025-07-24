#![deny(clippy::await_holding_refcell_ref)]

#[macro_use]
pub mod internal;

pub mod comm;
pub mod connection;
pub mod control;
pub mod events;
pub mod gateway;
pub mod hwstats;
pub mod launcher;
pub mod program;

pub use crate::internal::common::WrappedRcRefCell;
pub use crate::internal::common::index::{AsIdVec, ItemId};
pub use crate::internal::common::taskgroup::TaskGroup;
pub use crate::internal::common::utils::format_comma_delimited;
pub use crate::internal::common::{Map, Set};

pub use crate::internal::common::ids::{InstanceId, JobId, JobTaskId, TaskId, WorkerId};

pub type JobTaskCount = u32;

// Priority: Bigger number -> Higher priority
pub type Priority = i32;
pub type PriorityTuple = (Priority, Priority); // user priority, scheduler priority

pub type Error = internal::common::error::DsError;
pub type Result<T> = std::result::Result<T, Error>;

pub const MAX_FRAME_SIZE: usize = 128 * 1024 * 1024;

pub mod resources {
    pub use crate::internal::common::resources::{
        AMD_GPU_RESOURCE_NAME, Allocation, AllocationRequest, CPU_RESOURCE_ID, CPU_RESOURCE_NAME,
        MEM_RESOURCE_NAME, NVIDIA_GPU_RESOURCE_NAME, NumOfNodes, ResourceAllocRequest,
        ResourceAllocation, ResourceAmount, ResourceDescriptor, ResourceDescriptorCoupling,
        ResourceDescriptorItem, ResourceDescriptorKind, ResourceFractions, ResourceIndex,
        ResourceLabel, ResourceRequest, ResourceRequestEntries, ResourceRequestVariants,
        ResourceUnits, TimeRequest,
    };

    pub use crate::internal::common::resources::map::ResourceMap;

    pub use crate::internal::common::resources::descriptor::DescriptorError;

    pub use crate::internal::common::resources::amount::{
        FRACTIONS_MAX_DIGITS, FRACTIONS_PER_UNIT,
    };
}

pub mod server {
    pub use crate::control::server_start;
    pub use crate::internal::server::dataobj::ObjsToRemoveFromWorkers;
    pub use crate::internal::server::explain::{TaskExplainItem, TaskExplanation};
    pub use crate::internal::server::rpc::ConnectionDescriptor;
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

pub mod datasrv {
    pub use crate::internal::datasrv::dataobj::{DataInputId, DataObjectId, OutputId};
    pub use crate::internal::datasrv::local_client::LocalDataClient;
}

pub mod tests {
    pub use crate::internal::tests::*;
}
