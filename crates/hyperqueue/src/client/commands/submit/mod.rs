pub mod command;
pub mod defs;
pub mod directives;
mod jobfile;

pub use command::SubmitJobTaskConfOpts;
pub use command::{submit_computation, JobSubmitOpts};

pub use jobfile::{submit_computation_from_job_file, JobSubmitFileOpts};
