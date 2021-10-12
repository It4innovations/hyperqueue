use crate::common::manager::info::ManagerType;
use bstr::ByteSlice;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use crate::server::autoalloc::AutoAllocResult;

/// Name of a script that will be submitted to Slurm/PBS.
pub(super) const SUBMIT_SCRIPT_NAME: &str = "hq_submit.sh";

/// Name of a file that will store the job id of a submitted Slurm/PBS allocation.
pub(super) const JOBID_FILE_NAME: &str = "jobid";

pub fn create_allocation_dir(
    server_directory: PathBuf,
    name: &str,
) -> Result<PathBuf, std::io::Error> {
    let mut dir = server_directory;
    dir.push("autoalloc");
    dir.push(name);

    std::fs::create_dir_all(&dir)?;

    Ok(tempdir::TempDir::new_in(dir, "allocation")?.into_path())
}

pub fn check_command_output(output: Output) -> AutoAllocResult<Output> {
    let status = output.status;
    if !status.success() {
        return Err(anyhow::anyhow!(
            "Exit code: {}\nStderr: {}\nStdout: {}",
            status.code().unwrap_or(-1),
            output.stderr.to_str().unwrap_or("Invalid UTF-8"),
            output.stdout.to_str().unwrap_or("Invalid UTF-8")
        ));
    }
    Ok(output)
}

fn get_default_worker_idle_time() -> Duration {
    Duration::from_secs(5 * 60)
}

pub fn build_worker_args(hq_path: &Path, manager: ManagerType, server_dir: &Path) -> String {
    let manager = match manager {
        ManagerType::Pbs => "pbs",
        ManagerType::Slurm => "slurm",
    };

    let duration = humantime::format_duration(get_default_worker_idle_time()).to_string();
    format!(
        "{} worker start --idle-timeout {} --manager {} --server-dir {}",
        hq_path.display(),
        duration,
        manager,
        server_dir.display()
    )
}
