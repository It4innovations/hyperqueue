#![deny(clippy::await_holding_refcell_ref)]

#[macro_use]
pub mod common;
pub mod messages;
pub mod scheduler;
pub mod server;
pub mod transfer;
pub mod worker;

define_id_type!(WorkerId, u32);
define_id_type!(TaskId, u64);
define_id_type!(InstanceId, u32);

// Priority: Bigger number -> Higher priority
pub type Priority = i32;
pub type PriorityTuple = (Priority, Priority); // user priority, scheduler priority

pub type Error = crate::common::error::DsError;
pub type Result<T> = std::result::Result<T, Error>;

pub mod tests;
