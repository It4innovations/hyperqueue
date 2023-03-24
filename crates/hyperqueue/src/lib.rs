#![deny(clippy::await_holding_refcell_ref)]

pub mod client;
pub mod common;
#[cfg(feature = "dashboard")]
pub mod dashboard;
pub mod server;
pub mod stream;
pub mod transfer;
pub mod worker;

#[cfg(test)]
pub(crate) mod tests;

pub use tako::{Map, Set};

pub type Error = crate::common::error::HqError;
pub type Result<T> = std::result::Result<T, Error>;

// ID types
use tako::define_id_type;

pub use tako::WorkerId;
pub type TakoTaskId = tako::TaskId;
pub type Priority = tako::Priority;

define_id_type!(JobId, u32);
define_id_type!(JobTaskId, u32);

pub type JobTaskCount = u32;
pub type JobTaskStep = u32;

pub const DEFAULT_WORKER_GROUP_NAME: &str = "default";

// Reexports
pub use tako;
pub use tako::WrappedRcRefCell;
