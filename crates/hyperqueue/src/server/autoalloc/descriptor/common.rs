use crate::common::manager::info::ManagerType;
use anyhow::Context;
use bstr::ByteSlice;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;
use tokio::process::Command;

use crate::server::autoalloc::state::AllocationId;
use crate::server::autoalloc::{AutoAllocResult, DescriptorId};

/// Name of a script that will be submitted to Slurm/PBS.
const SUBMIT_SCRIPT_NAME: &str = "hq_submit.sh";

/// Name of a file that will store the job id of a submitted Slurm/PBS allocation.
const JOBID_FILE_NAME: &str = "jobid";

pub struct ExternalHandler {
    pub server_directory: PathBuf,
    pub hq_path: PathBuf,
    pub name: Option<String>,
    allocation_counter: u64,
}

impl ExternalHandler {
    pub fn new(server_directory: PathBuf, name: Option<String>) -> anyhow::Result<Self> {
        let hq_path = std::env::current_exe().context("Cannot get HyperQueue path")?;
        Ok(Self {
            server_directory,
            hq_path,
            name,
            allocation_counter: 0,
        })
    }

    pub fn create_allocation_id(&mut self) -> u64 {
        let id = self.allocation_counter;
        self.allocation_counter += 1;
        id
    }
}

pub fn create_allocation_dir(
    server_directory: PathBuf,
    id: DescriptorId,
    name: Option<&String>,
    allocation_num: u64,
) -> Result<PathBuf, std::io::Error> {
    let mut dir = server_directory;
    dir.push("autoalloc");
    if let Some(name) = name {
        dir.push(name);
    } else {
        dir.push(id.to_string());
    }
    dir.push(allocation_num.to_string());

    std::fs::create_dir_all(&dir)?;

    Ok(dir)
}

/// Submits a script into PBS/Slurm and creates debug information in the given allocation `directory`.
pub async fn submit_script<F>(
    script: String,
    program: &str,
    directory: &Path,
    get_job_id: F,
) -> AutoAllocResult<AllocationId>
where
    F: FnOnce(&str) -> AutoAllocResult<AllocationId>,
{
    let script_path = directory.join(SUBMIT_SCRIPT_NAME);
    let script_path = script_path.to_str().unwrap();

    std::fs::write(&script_path, script)
        .with_context(|| anyhow::anyhow!("Cannot write script into {}", script_path))?;

    let arguments = vec![program, script_path];

    log::debug!("Running command `{}`", arguments.join(" "));
    let mut command = Command::new(&arguments[0]);
    command.args(&arguments[1..]);

    let output = command
        .output()
        .await
        .with_context(|| format!("{} start failed", program))?;
    let output =
        check_command_output(output).with_context(|| format!("{} execution failed", program))?;
    let output = output
        .stdout
        .to_str()
        .map_err(|e| anyhow::anyhow!("Invalid UTF-8 {} output: {:?}", program, e))?
        .trim();

    let job_id = get_job_id(output)?;

    // Write the job id to the allocation directory as a debug information
    std::fs::write(directory.join(JOBID_FILE_NAME), &job_id)?;

    Ok(job_id)
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
