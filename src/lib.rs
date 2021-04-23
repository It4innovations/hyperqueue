pub mod common;
pub mod server;
pub mod client;
pub mod transfer;
pub mod worker;

pub type Map<K, V> = hashbrown::HashMap<K, V>;
pub type Set<T> = hashbrown::HashSet<T>;

pub type WorkerId = u64;
pub type TaskId = u64;
pub type TaskTypeId = u32;

pub type Error = crate::common::error::HqError;
pub type Result<T> = std::result::Result<T, Error>;
