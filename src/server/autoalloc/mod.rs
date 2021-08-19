//! This module controls autoalloc (automatic allocation), a background process that queues jobs
//! into PBS/Slurm in response to user requirements and task workload to provide more workers for
//! the HQ runtime.
//!
//! The term `allocation` represents a PBS/Slurm job in this module, to distinguish itself from
//! HQ jobs.
use thiserror::Error;

pub use process::autoalloc_process;
pub use state::AutoAllocState;

mod descriptor;
mod process;
mod state;

#[derive(Debug, Error)]
pub enum AutoAllocError {
    #[error("Descriptor named {0} already exists")]
    DescriptorAlreadyExists(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Submit failed: {0}")]
    SubmitFailed(String),
    #[error("{0}")]
    Custom(String),
}

pub type AutoAllocResult<T> = Result<T, AutoAllocError>;

pub use descriptor::pbs::PbsDescriptor;
