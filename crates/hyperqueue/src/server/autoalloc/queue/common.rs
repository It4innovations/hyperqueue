use crate::common::manager::info::ManagerType;
use crate::common::utils::fs;
use crate::server::autoalloc::state::{AllocationId, AllocationWorkdir};
use crate::server::autoalloc::{AutoAllocResult, QueueId, QueueParameters};
use anyhow::Context;
use bstr::ByteSlice;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;
use tokio::process::Command;

/// Name of a script that will be submitted to Slurm/PBS.
const SUBMIT_SCRIPT_NAME: &str = "hq-submit.sh";

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
        let hq_path = fs::get_hq_binary_path()?;
        Ok(Self {
            server_directory,
            hq_path,
            name,
            allocation_counter: 0,
        })
    }

    pub fn create_allocation_id(&mut self) -> u64 {
        self.allocation_counter += 1;
        self.allocation_counter
    }
}

pub fn create_allocation_dir(
    server_directory: PathBuf,
    id: QueueId,
    name: Option<&String>,
    allocation_num: u64,
) -> Result<AllocationWorkdir, std::io::Error> {
    let mut dir = server_directory;
    dir.push("autoalloc");

    let mut dir_name = id.to_string();
    if let Some(name) = name {
        dir_name.push('-');
        dir_name.push_str(name);
    }

    dir.push(dir_name);
    dir.push(format!("{allocation_num:03}"));

    std::fs::create_dir_all(&dir)?;

    Ok(dir.into())
}

/// Creates a name for an external allocation, based on the allocation counter
/// and an optional name prefix.
pub fn format_allocation_name(name: Option<String>, queue_id: u32, allocation_id: u64) -> String {
    let mut name = name.unwrap_or_else(|| format!("hq-{queue_id}"));
    name.push_str(&format!("-{allocation_id}"));
    name
}

/// Submits a script into PBS/Slurm and creates debug information in the given allocation `directory`.
pub async fn submit_script<F>(
    script: String,
    program: &str,
    directory: &AllocationWorkdir,
    get_job_id: F,
) -> AutoAllocResult<AllocationId>
where
    F: FnOnce(&str) -> AutoAllocResult<AllocationId>,
{
    let directory = directory.as_ref();
    let script_path = directory.join(SUBMIT_SCRIPT_NAME);
    let script_path = script_path.to_str().unwrap();

    std::fs::write(script_path, script)
        .with_context(|| anyhow::anyhow!("Cannot write script into {}", script_path))?;

    let arguments = vec![program, script_path];

    log::debug!("Running command `{}`", arguments.join(" "));
    let mut command = create_command(arguments, directory);

    let output = command
        .output()
        .await
        .with_context(|| format!("{program} start failed"))?;
    let output =
        check_command_output(output).with_context(|| format!("{program} execution failed"))?;
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

pub fn create_command(arguments: Vec<&str>, workdir: &Path) -> Command {
    let mut command = Command::new(arguments[0]);
    command.args(&arguments[1..]);
    command.current_dir(workdir);
    command
}

pub fn check_command_output(output: Output) -> AutoAllocResult<Output> {
    let status = output.status;
    if !status.success() {
        return Err(anyhow::anyhow!(
            "Exit code: {}\nStderr: {}\nStdout: {}",
            status.code().unwrap_or(-1),
            output.stderr.to_str_lossy().trim(),
            output.stdout.to_str_lossy().trim()
        ));
    }
    Ok(output)
}

fn get_default_worker_idle_time() -> Duration {
    Duration::from_secs(5 * 60)
}

pub fn build_worker_args(
    hq_path: &Path,
    manager: ManagerType,
    server_dir: &Path,
    params: &QueueParameters,
) -> String {
    let manager = match manager {
        ManagerType::Pbs => "pbs",
        ManagerType::Slurm => "slurm",
    };

    let idle_timeout = params
        .idle_timeout
        .unwrap_or_else(get_default_worker_idle_time);
    let idle_timeout = humantime::format_duration(idle_timeout);

    let mut env = String::new();
    if let Ok(log_env) = std::env::var("RUST_LOG") {
        write!(env, "RUST_LOG={log_env} ").unwrap();
    }

    let mut args = format!(
        "{env}{hq} worker start --idle-timeout \"{idle_timeout}\" --manager \"{manager}\" --server-dir \"{server_dir}\"",
        hq = hq_path.display(),
        server_dir = server_dir.display()
    );

    if !params.worker_args.is_empty() {
        args.push_str(&format!(" {}", params.worker_args.join(" ")));
    }

    args
}

pub fn wrap_worker_cmd(
    mut worker_cmd: String,
    worker_start_cmd: Option<&str>,
    worker_stop_cmd: Option<&str>,
) -> String {
    if let Some(start_cmd) = worker_start_cmd {
        worker_cmd = format!("{start_cmd} && {worker_cmd}");
    }
    if let Some(stop_cmd) = worker_stop_cmd {
        write!(worker_cmd, "; {stop_cmd}").unwrap();
    }

    worker_cmd
}

#[cfg(test)]
mod tests {
    use crate::common::utils::fs::normalize_exe_path;
    use crate::server::autoalloc::queue::common::wrap_worker_cmd;
    use std::path::PathBuf;

    #[test]
    fn wrap_cmd_noop() {
        assert_eq!(
            wrap_worker_cmd("foo".to_string(), None, None),
            "foo".to_string()
        );
    }

    #[test]
    fn wrap_cmd_start() {
        assert_eq!(
            wrap_worker_cmd("foo bar".to_string(), Some("init.sh"), None),
            "init.sh && foo bar".to_string()
        );
    }

    #[test]
    fn wrap_cmd_stop() {
        assert_eq!(
            wrap_worker_cmd("foo bar".to_string(), None, Some("unload.sh")),
            "foo bar; unload.sh".to_string()
        );
    }

    #[test]
    fn wrap_cmd_start_stop() {
        assert_eq!(
            wrap_worker_cmd("foo bar".to_string(), Some("init.sh"), Some("unload.sh")),
            "init.sh && foo bar; unload.sh".to_string()
        );
    }

    #[test]
    fn normalize_deleted_path() {
        assert_eq!(
            normalize_exe_path(PathBuf::from("/a/b/c/hq (deleted)"),),
            PathBuf::from("/a/b/c/hq")
        );
    }
}
