use thiserror::Error;

pub use descriptor::create_pbs_descriptor;
pub use process::autoalloc_process;
pub use state::AutoAllocState;

mod descriptor;
mod process;
mod state;

#[derive(Debug, Error)]
pub enum AutoAllocError {
    #[error("Descriptor named {0} already exists")]
    DescriptorAlreadyExists(String),
}

pub type AutoAllocResult<T> = Result<T, AutoAllocError>;
