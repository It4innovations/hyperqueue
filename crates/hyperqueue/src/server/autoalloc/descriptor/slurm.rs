use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use bstr::ByteSlice;
use tokio::process::Command;

use crate::common::manager::info::ManagerType;
use crate::common::manager::slurm::{
    format_slurm_duration, get_scontrol_items, parse_slurm_datetime,
};
use crate::common::timeutils::local_to_system_time;
use crate::server::autoalloc::descriptor::common::{build_worker_args, SUBMIT_SCRIPT_NAME};
use crate::server::autoalloc::descriptor::{common, CreatedAllocation, QueueHandler};
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::{AutoAllocResult, DescriptorId, QueueInfo};

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
        let hq_path = self.hq_path.clone();
        let server_directory = self.server_directory.clone();
        let name = descriptor_id.to_string();
        let sbatch_args = self.sbatch_args.clone();

        Box::pin(async move {
            // TODO: unify code with PBS
            let directory = common::create_allocation_dir(server_directory.clone(), &name)?;
            let script_path = directory.join(SUBMIT_SCRIPT_NAME);
            let script_path = script_path.to_str().unwrap();

            let worker_args = build_worker_args(&hq_path, ManagerType::Slurm, &server_directory);
            let script = build_slurm_submit_script(
                worker_count,
                timelimit.as_ref(),
                &partition,
                &format!("hq-alloc-{}", descriptor_id),
                &directory.join("stdout").display().to_string(),
                &directory.join("stderr").display().to_string(),
                &sbatch_args.join(" "),
                &worker_args,
            );
            std::fs::write(&script_path, script).with_context(|| {
                anyhow::anyhow!("Cannot write Slurm script into {}", script_path)
            })?;

            let arguments = vec!["sbatch", script_path];

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
                .nth(3)
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
                "RUNNING" | "COMPLETING" => {
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

#[allow(clippy::too_many_arguments)]
fn build_slurm_submit_script(
    nodes: u64,
    timelimit: Option<&Duration>,
    partition: &str,
    name: &str,
    stdout: &str,
    stderr: &str,
    sbatch_args: &str,
    worker_cmd: &str,
) -> String {
    let mut script = format!(
        r##"#!/bin/bash
#SBATCH --nodes={nodes}
#SBATCH --partition={partition}
#SBATCH --job-name={name}
#SBATCH --output={stdout}
#SBATCH --error={stderr}
"##,
        nodes = nodes,
        partition = partition,
        name = name,
        stdout = stdout,
        stderr = stderr,
    );

    if let Some(timelimit) = timelimit {
        script.push_str(&format!(
            "#SBATCH --time={}\n",
            format_slurm_duration(timelimit)
        ));
    }

    if !sbatch_args.is_empty() {
        script.push_str(&format!("#SBATCH {}\n", sbatch_args));
    }

    script.push_str(&format!("\n{}", worker_cmd));
    script
}
