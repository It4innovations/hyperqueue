use std::time::Duration;

use crate::common::manager::pbs::{format_pbs_duration, parse_pbs_datetime};
use crate::common::timeutils::local_datetime_to_system_time;
use crate::server::autoalloc::descriptor::{CreatedAllocation, QueueHandler};
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::AutoAllocResult;
use anyhow::Context;
use bstr::ByteSlice;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Output;
use tokio::process::Command;

// TODO: pass as queue info in trait
pub struct PbsHandler {
    queue: String,
    timelimit: Option<Duration>,
    server_directory: PathBuf,
    name: String,
    hq_path: PathBuf,
}

impl PbsHandler {
    pub fn new(
        queue: String,
        timelimit: Option<Duration>,
        name: String,
        server_directory: PathBuf,
    ) -> Self {
        let hq_path = std::env::current_exe().expect("Cannot get HyperQueue path");
        Self {
            queue,
            timelimit,
            server_directory,
            name,
            hq_path,
        }
    }
}

fn create_allocation_dir(server_directory: PathBuf, name: &str) -> Result<PathBuf, std::io::Error> {
    let mut dir = server_directory;
    dir.push("autoalloc");
    dir.push(name);

    std::fs::create_dir_all(&dir)?;

    Ok(tempdir::TempDir::new_in(dir, "allocation")?.into_path())
}

impl QueueHandler for PbsHandler {
    fn schedule_allocation(
        &self,
        worker_count: u64,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<CreatedAllocation>>>> {
        let queue = self.queue.clone();
        let timelimit = self.timelimit;
        let hq_path = self.hq_path.display().to_string();
        let server_directory = self.server_directory.clone();
        let name = self.name.clone();

        Box::pin(async move {
            let directory = create_allocation_dir(server_directory.clone(), &name)?;

            let mut command = Command::new("qsub");
            command
                .arg("-q")
                .arg(queue.as_str())
                .arg("-wd")
                .arg(directory.display().to_string())
                .arg("-o")
                .arg(directory.join("stdout").display().to_string())
                .arg("-e")
                .arg(directory.join("stderr").display().to_string())
                .arg(format!("-lselect={}", worker_count));

            if let Some(ref timelimit) = timelimit {
                command.arg(format!("-lwalltime={}", format_pbs_duration(timelimit)));
            }

            // `hq worker` arguments
            command.arg("--");
            command.arg(hq_path);
            command.args([
                "worker",
                "start",
                "--idle-timeout",
                "10m",
                "--manager",
                "pbs",
                "--server-dir",
                &server_directory.display().to_string(),
            ]);

            log::debug!("Running PBS command {:?}", command);

            let output = command.output().await.context("qsub start failed")?;
            let output = check_command_output(output).context("qsub execution failed")?;

            let job_id = output
                .stdout
                .to_str()
                .map_err(|e| anyhow::anyhow!("Invalid UTF-8 qsub output: {:?}", e))?
                .trim();
            let job_id = job_id.to_string();

            // Write the PBS job id to the folder as a debug information
            std::fs::write(directory.join("jobid"), &job_id)?;

            AutoAllocResult::Ok(CreatedAllocation {
                id: job_id,
                working_dir: directory,
            })
        })
    }

    fn get_allocation_status(
        &self,
        allocation_id: AllocationId,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<Option<AllocationStatus>>>>> {
        Box::pin(async move {
            let mut command = Command::new("qstat");
            // -x will also display finished jobs
            command.args(["-f", &allocation_id, "-F", "json", "-x"]);

            let output = command.output().await.context("qstat start failed")?;
            let output = check_command_output(output).context("qstat execution failed")?;
            let data: serde_json::Value =
                serde_json::from_slice(&output.stdout).context("Cannot parse qstat JSON output")?;
            let job = &data["Jobs"][&allocation_id];
            if job.is_null() {
                // Job not found
                return Ok(None);
            }

            let state = get_json_str(&job["job_state"], "Job state")?;

            let status = match state {
                "Q" => {
                    let queue_time = get_json_str(&job["qtime"], "Queue time")?;
                    AllocationStatus::Queued {
                        queued_at: local_datetime_to_system_time(parse_pbs_datetime(queue_time)?),
                    }
                }
                "R" => {
                    let start_time = get_json_str(&job["stime"], "Start time")?;
                    AllocationStatus::Running {
                        started_at: local_datetime_to_system_time(parse_pbs_datetime(start_time)?),
                    }
                }
                "F" => {
                    let exit_status = get_json_number(&job["Exit_status"], "Exit status")?;
                    if exit_status == 0 {
                        AllocationStatus::Finished
                    } else {
                        AllocationStatus::Failed
                    }
                }
                status => anyhow::bail!("Unknown PBS job status {}", status),
            };

            Ok(Some(status))
        })
    }
}

fn get_json_str<'a>(value: &'a serde_json::Value, context: &str) -> AutoAllocResult<&'a str> {
    value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("JSON key {} not found", context))
}
fn get_json_number(value: &serde_json::Value, context: &str) -> AutoAllocResult<u64> {
    value
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("JSON key {} not found", context))
}

fn check_command_output(output: Output) -> AutoAllocResult<Output> {
    let status = output.status;
    if !status.success() {
        return Err(anyhow::anyhow!(
            "Exit code {}, stderr: {}, stdout: {}",
            status.code().unwrap(),
            output.stderr.to_str().unwrap(),
            output.stdout.to_str().unwrap()
        ));
    }
    Ok(output)
}
