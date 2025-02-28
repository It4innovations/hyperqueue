//! This module controls autoalloc (automatic allocation), a background process that queues jobs
//! into PBS/Slurm in response to user requirements and task workload to provide more workers for
//! the HQ runtime.
//!
//! The term `allocation` represents a PBS/Slurm job in this module, to distinguish itself from
//! HQ jobs.
mod config;
mod estimator;
mod process;
mod queue;
mod service;
mod state;

pub type AutoAllocResult<T> = anyhow::Result<T>;

pub use process::try_submit_allocation;
pub use queue::QueueInfo;
pub use service::{AutoAllocService, LostWorkerDetails, create_autoalloc_service};
pub use state::{Allocation, AllocationId, AllocationState, QueueId};

#[cfg(test)]
pub use service::tests::test_alloc_service;
