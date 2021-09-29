use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::SystemTime;

use anyhow::Context;
use bstr::ByteSlice;
use tokio::process::Command;

use crate::common::manager::slurm::{format_slurm_duration, parse_slurm_datetime};
use crate::common::timeutils::local_to_system_time;
use crate::server::autoalloc::descriptor::{common, CreatedAllocation, QueueHandler};
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::{AutoAllocResult, DescriptorId, QueueInfo};
use crate::Map;

pub struct SlurmHandler {
    server_directory: PathBuf,
    hq_path: PathBuf,
    sbatch_args: Vec<String>,
}

impl SlurmHandler {
    pub async fn new(server_directory: PathBuf, sbatch_args: Vec<String>) -> anyhow::Result<Self> {
        let hq_path = std::env::current_exe().context("Cannot get HyperQueue path")?;
        Ok(Self {
            server_directory,
            hq_path,
            sbatch_args,
        })
    }
}

impl QueueHandler for SlurmHandler {
    fn schedule_allocation(
        &self,
        descriptor_id: DescriptorId,
        queue_info: &QueueInfo,
        worker_count: u64,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<CreatedAllocation>>>> {
        let partition = queue_info.queue.clone();
        let timelimit = queue_info.timelimit;
        let hq_path = self.hq_path.display().to_string();
        let server_directory = self.server_directory.clone();
        let name = descriptor_id.to_string();
        let sbatch_args = self.sbatch_args.clone();

        Box::pin(async move {
            let directory = common::create_allocation_dir(server_directory.clone(), &name)?;

            let mut arguments = vec![
                "sbatch".to_string(),
                "--partition".to_string(),
                partition,
                "--output".to_string(),
                directory.join("stdout").display().to_string(),
                "--error".to_string(),
                directory.join("stderr").display().to_string(),
                format!("--nodes={}", worker_count),
            ];

            if let Some(ref timelimit) = timelimit {
                arguments.push(format!("--time={}", format_slurm_duration(timelimit)));
            }

            arguments.extend(sbatch_args);

            // TODO: unify somehow with PBS
            // `hq worker` arguments
            let worker_args = vec![
                hq_path,
                "worker".to_string(),
                "start".to_string(),
                "--idle-timeout".to_string(),
                "10m".to_string(),
                "--manager".to_string(),
                "slurm".to_string(),
                "--server-dir".to_string(),
                server_directory.display().to_string(),
            ];
            arguments.extend(["--wrap".to_string(), worker_args.join(" ")]);

            log::debug!("Running Slurm command `{}`", arguments.join(" "));
            let mut command = Command::new(&arguments[0]);
            command.args(&arguments[1..]);

            let output = command.output().await.context("sbatch start failed")?;
            let output = common::check_command_output(output).context("sbatch execution failed")?;

            let output = output
                .stdout
                .to_str()
                .map_err(|e| anyhow::anyhow!("Invalid UTF-8 qsub output: {:?}", e))?
                .trim();
            let job_id = output
                .split(' ')
                .skip(3)
                .next()
                .ok_or_else(|| anyhow::anyhow!("Missing job id in sbatch output"))?;

            let job_id = job_id.to_string();

            // Write the Slurm job id to the folder as a debug information
            std::fs::write(directory.join("jobid"), &job_id)?;

            Ok(CreatedAllocation {
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
            let arguments = vec!["scontrol", "show", "job", &allocation_id];
            log::debug!("Running Slurm command `{}`", arguments.join(" "));

            let mut command = Command::new(arguments[0]);
            command.args(&arguments[1..]);

            let output = command.output().await.context("scontrol start failed")?;
            let output =
                common::check_command_output(output).context("scontrol execution failed")?;

            let output = output
                .stdout
                .to_str()
                .map_err(|err| anyhow::anyhow!("Invalid UTF-8 in scontrol output: {:?}", err))?;
            let items = get_scontrol_items(output);

            let get_key = |key: &str| -> AutoAllocResult<&str> {
                let value = items.get(key);
                value
                    .ok_or_else(|| anyhow::anyhow!("Missing key {} in Slurm scontrol output", key))
                    .map(|v| *v)
            };
            let parse_time = |time: &str| -> AutoAllocResult<SystemTime> {
                Ok(local_to_system_time(parse_slurm_datetime(time).map_err(
                    |err| anyhow::anyhow!("Cannot parse Slurm datetime {}: {:?}", time, err),
                )?))
            };

            let status = get_key("JobState")?;
            let status = match status {
                "PENDING" | "CONFIGURING" => AllocationStatus::Queued,
                "RUNNING" => {
                    let started_at = parse_time(get_key("StartTime")?)?;
                    AllocationStatus::Running { started_at }
                }
                "COMPLETED" | "CANCELLED" | "FAILED" | "TIMEOUT" => {
                    let finished = matches!(status, "COMPLETED" | "TIMEOUT");
                    let started_at = parse_time(get_key("StartTime")?)?;
                    let finished_at = parse_time(get_key("EndTime")?)?;

                    if finished {
                        AllocationStatus::Finished {
                            started_at,
                            finished_at,
                        }
                    } else {
                        AllocationStatus::Failed {
                            started_at,
                            finished_at,
                        }
                    }
                }
                _ => anyhow::bail!("Unknown Slurm job status {}", status),
            };

            Ok(Some(status))
        })
    }

    fn remove_allocation(
        &self,
        allocation_id: AllocationId,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>> {
        Box::pin(async move {
            let arguments = vec!["scancel", &allocation_id];
            log::debug!("Running Slurm command `{}`", arguments.join(" "));
            let mut command = Command::new(&arguments[0]);
            command.args(&arguments[1..]);

            let output = command.output().await?;
            common::check_command_output(output)?;
            Ok(())
        })
    }
}

/// Parse <key>=<value> pairs from the output of `scontrol show job <job-id>`.
fn get_scontrol_items(output: &str) -> Map<&str, &str> {
    let mut map = Map::new();
    for line in output.lines() {
        for item in line.trim().split(' ') {
            let iter: Vec<_> = item.split('=').take(2).collect();
            if iter.len() < 2 {
                continue;
            }
            let (key, value) = (iter[0], iter[1]);
            map.insert(key, value);
        }
    }

    map
}
