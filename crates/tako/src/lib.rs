#[macro_use]
pub mod common;
pub mod transfer;
pub mod messages;
pub mod server;
pub mod worker;
pub mod scheduler2;

pub type WorkerId = u64;
pub type TaskId = u64;
pub type TaskTypeId = u32;

// Priority: Bigger number -> Higher priority
// TODO: This is meaning is not synchronized in all code!! We need to FIX this
pub type PriorityValue = i32;
pub type Priority = (PriorityValue, PriorityValue);  // TODO: Rename PriorityValue -> Priority, Priority -> PriotityTuple

pub type Error = crate::common::error::DsError;
pub type Result<T> = std::result::Result<T, Error>;
