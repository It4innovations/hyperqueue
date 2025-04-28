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

use serde::{Deserialize, Serialize};

pub type Error = crate::common::error::HqError;
pub type Result<T> = std::result::Result<T, Error>;

// ID types
use tako::{JobId, JobTaskId};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobDataObjectId {
    pub task_id: JobTaskId,
    pub output_id: OutputId,
}

impl JobDataObjectId {
    pub fn to_dataobj_id(&self, job_id: JobId) -> DataObjectId {
        DataObjectId::new(tako::TaskId::new(job_id, self.task_id), self.output_id)
    }
}

pub const DEFAULT_WORKER_GROUP_NAME: &str = "default";

// Reexports
pub use tako;
use tako::datasrv::{DataObjectId, OutputId};
pub use tako::WrappedRcRefCell;

pub const HQ_VERSION: &str = {
    match option_env!("HQ_BUILD_VERSION") {
        Some(version) => version,
        None => const_format::concatcp!(env!("CARGO_PKG_VERSION"), "-dev"),
    }
};
