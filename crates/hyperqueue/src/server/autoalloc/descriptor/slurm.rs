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
use crate::server::autoalloc::descriptor::common::{
    build_worker_args, create_allocation_dir, submit_script, ExternalHandler,
};
use crate::server::autoalloc::descriptor::{common, CreatedAllocation, QueueHandler};
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::{AutoAllocResult, DescriptorId, QueueInfo};

pub struct SlurmHandler {
    handler: ExternalHandler,
}

impl SlurmHandler {
    pub fn new(server_directory: PathBuf, name: Option<String>) -> anyhow::Result<Self> {
        let handler = ExternalHandler::new(server_directory, name)?;
        Ok(Self { handler })
    }
}

impl QueueHandler for SlurmHandler {
    fn schedule_allocation(
        &mut self,
        descriptor_id: DescriptorId,
        queue_info: &QueueInfo,
        worker_count: u64,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<CreatedAllocation>>>> {
        let queue_info = queue_info.clone();
        let timelimit = queue_info.timelimit;
        let hq_path = self.handler.hq_path.clone();
        let server_directory = self.handler.server_directory.clone();
        let name = self.handler.name.clone();
        let allocation_num = self.handler.create_allocation_id();

        Box::pin(async move {
            let directory = create_allocation_dir(
                server_directory.clone(),
                descriptor_id,
                name.as_ref(),
                allocation_num,
            )?;

            let worker_args =
                build_worker_args(&hq_path, ManagerType::Slurm, &server_directory, &queue_info);
            let script = build_slurm_submit_script(
                worker_count,
                timelimit,
                &format!("hq-alloc-{}", descriptor_id),
                &directory.join("stdout").display().to_string(),
                &directory.join("stderr").display().to_string(),
                &queue_info.additional_args.join(" "),
                &worker_args,
            );
            let job_id = submit_script(script, "sbatch", &directory, |output| {
                output
                    .split(' ')
                    .nth(3)
                    .ok_or_else(|| anyhow::anyhow!("Missing job id in sbatch output"))
                    .map(|id| id.to_string())
            })
            .await?;

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
    timelimit: Duration,
    name: &str,
    stdout: &str,
    stderr: &str,
    sbatch_args: &str,
    worker_cmd: &str,
) -> String {
    let mut script = format!(
        r##"#!/bin/bash
#SBATCH --nodes={nodes}
#SBATCH --job-name={name}
#SBATCH --output={stdout}
#SBATCH --error={stderr}
#SBATCH --time={walltime}
"##,
        nodes = nodes,
        name = name,
        stdout = stdout,
        stderr = stderr,
        walltime = format_slurm_duration(&timelimit)
    );

    if !sbatch_args.is_empty() {
        script.push_str(&format!("#SBATCH {}\n", sbatch_args));
    }

    script.push_str(&format!("\n{}", worker_cmd));
    script
}
