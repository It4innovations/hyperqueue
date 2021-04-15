#[macro_use]
pub mod common;
pub mod messages;
pub mod scheduler;
pub mod server;
pub mod transfer;
pub mod worker;

pub type WorkerId = u64; // Maybe non-zero type for optimizing Option<WorkerId>?
pub type TaskId = u64;
pub type TaskTypeId = u32;
pub type OutputId = u32;

// Priority: Bigger number -> Higher priority
pub type Priority = i32;
pub type PriorityTuple = (Priority, Priority); // user priority, scheduler priority

pub type Error = crate::common::error::DsError;
pub type Result<T> = std::result::Result<T, Error>;
