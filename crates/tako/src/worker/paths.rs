use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct WorkerPaths {
    /// Used for storing trace/profiling/log information.
    pub work_dir: PathBuf,
    /// Used for local communication (unix socket).
    pub local_dir: PathBuf,
}

impl WorkerPaths {
    pub fn new(work_dir: PathBuf, local_dir: PathBuf) -> Self {
        Self {
            work_dir,
            local_dir,
        }
    }
}
