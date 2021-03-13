#[macro_use]
pub mod common;
pub(crate) mod transfer;
pub(crate) mod scheduler;
pub(crate) mod messages;
pub mod server;
pub mod worker;

pub type WorkerId = u64;
pub type TaskId = u64;
pub type PriorityValue = i32;
pub type Priority = (PriorityValue, PriorityValue);  // TODO: Rename PriorityValue -> Priority, Priority -> PriotityTuple

pub type Error = crate::common::error::DsError;
pub type Result<T> = std::result::Result<T, Error>;