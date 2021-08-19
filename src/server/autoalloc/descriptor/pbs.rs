use std::time::Duration;

use async_trait::async_trait;

use crate::common::manager::pbs::{format_pbs_duration, parse_pbs_datetime};
use crate::common::timeutils::local_datetime_to_system_time;
use crate::server::autoalloc::descriptor::QueueDescriptor;
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::AutoAllocResult;
use anyhow::Context;
use bstr::ByteSlice;
use std::path::PathBuf;
use std::process::Output;
use tokio::process::Command;

pub struct PbsDescriptor {
    max_workers_per_alloc: u32,
    target_worker_count: u32,
    queue: String,
    walltime: Option<Duration>,
    server_directory: PathBuf,
    name: String,
    hq_path: PathBuf,
}

impl PbsDescriptor {
    pub fn new(
        max_workers_per_alloc: u32,
        target_worker_count: u32,
        queue: String,
        walltime: Option<Duration>,
        name: String,
        server_directory: PathBuf,
    ) -> Self {
        let hq_path = std::env::current_exe().unwrap();
        Self {
            max_workers_per_alloc,
            target_worker_count,
            queue,
            walltime,
            server_directory,
            name,
            hq_path,
        }
    }

    fn create_allocation_dir(&self) -> Result<PathBuf, std::io::Error> {
        let mut dir = self.server_directory.clone();
        dir.push("autoalloc");
        dir.push(&self.name);

        std::fs::create_dir_all(&dir)?;

        Ok(tempdir::TempDir::new_in(dir, "allocation")?.into_path())
    }
}

#[async_trait(?Send)]
impl QueueDescriptor for PbsDescriptor {
    fn target_scale(&self) -> u32 {
        self.target_worker_count
    }

    fn max_workers_per_alloc(&self) -> u32 {
        self.max_workers_per_alloc
    }

    async fn schedule_allocation(&self, worker_count: u64) -> AutoAllocResult<AllocationId> {
        let directory = self.create_allocation_dir()?;

        let mut command = Command::new("qsub");
        command
            .arg("-q")
            .arg(self.queue.as_str())
            .arg("-wd")
            .arg(directory.display().to_string())
            .arg("-o")
            .arg(directory.join("stdout").display().to_string())
            .arg("-e")
            .arg(directory.join("stderr").display().to_string())
            .arg(format!("-lselect={}", worker_count));

        if let Some(ref walltime) = self.walltime {
            command.arg(format!("-lwalltime={}", format_pbs_duration(walltime)));
        }

        // `hq worker` arguments
        command.arg("--");
        command.arg(self.hq_path.display().to_string());
        command.args([
            "worker",
            "start",
            "--idle-timeout",
            "10m",
            "--manager",
            "pbs",
            "--server-dir",
            &self.server_directory.display().to_string(),
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

        AutoAllocResult::Ok(job_id)
    }

    async fn get_allocation_status(
        &self,
        allocation_id: &str,
    ) -> AutoAllocResult<AllocationStatus> {
        let mut command = Command::new("qstat");
        // -x will also display finished jobs
        command.args(["-f", allocation_id, "-F", "json", "-x"]);

        let output = command.output().await.context("qstat start failed")?;
        let output = check_command_output(output).context("qstat execution failed")?;
        let data: serde_json::Value =
            serde_json::from_slice(&output.stdout).context("Cannot parse qstat JSON output")?;
        let job = &data["Jobs"][allocation_id];
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

        Ok(status)
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
