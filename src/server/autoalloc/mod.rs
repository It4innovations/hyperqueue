use thiserror::Error;

pub use process::autoalloc_process;
pub use state::AutoAllocState;

mod descriptor;
mod process;
mod state;

#[derive(Debug, Error, Clone)]
pub enum AutoAllocError {
    #[error("Descriptor named {0} already exists")]
    DescriptorAlreadyExists(String),
    #[error("{0}")]
    Custom(String),
}

pub type AutoAllocResult<T> = Result<T, AutoAllocError>;
