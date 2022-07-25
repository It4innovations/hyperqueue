use crate::common::utils::fs::get_current_dir;
use anyhow::Context;
use bstr::ByteSlice;
use std::fmt::Write;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tako::Map;

use crate::common::manager::info::ManagerType;
use crate::common::manager::slurm::{
    format_slurm_duration, get_scontrol_items, parse_slurm_datetime,
};
use crate::common::utils::time::local_to_system_time;
use crate::server::autoalloc::queue::common::{
    build_worker_args, create_allocation_dir, create_command, submit_script, ExternalHandler,
};
use crate::server::autoalloc::queue::{
    common, AllocationExternalStatus, AllocationStatusMap, AllocationSubmissionResult,
    QueueHandler, SubmitMode,
};
use crate::server::autoalloc::{Allocation, AllocationId, AutoAllocResult, QueueId, QueueInfo};

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
    fn submit_allocation(
        &mut self,
        queue_id: QueueId,
        queue_info: &QueueInfo,
        worker_count: u64,
        _mode: SubmitMode,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationSubmissionResult>>>> {
        let queue_info = queue_info.clone();
        let timelimit = queue_info.timelimit;
        let hq_path = self.handler.hq_path.clone();
        let server_directory = self.handler.server_directory.clone();
        let name = self.handler.name.clone();
        let allocation_num = self.handler.create_allocation_id();

        Box::pin(async move {
            let working_dir = create_allocation_dir(
                server_directory.clone(),
                queue_id,
                name.as_ref(),
                allocation_num,
            )?;

            let worker_args =
                build_worker_args(&hq_path, ManagerType::Slurm, &server_directory, &queue_info);
            let script = build_slurm_submit_script(
                worker_count,
                timelimit,
                &format!("hq-alloc-{}", queue_id),
                &working_dir.join("stdout").display().to_string(),
                &working_dir.join("stderr").display().to_string(),
                &queue_info.additional_args.join(" "),
                &worker_args,
            );
            let id = submit_script(script, "sbatch", &working_dir, |output| {
                output
                    .split(' ')
                    .nth(3)
                    .ok_or_else(|| anyhow::anyhow!("Missing job id in sbatch output"))
                    .map(|id| id.to_string())
            })
            .await;

            Ok(AllocationSubmissionResult::new(id, working_dir))
        })
    }

    fn get_status_of_allocations(
        &self,
        allocations: &[&Allocation],
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationStatusMap>>>> {
        let allocation_ids: Vec<AllocationId> =
            allocations.iter().map(|alloc| alloc.id.clone()).collect();
        let workdir = allocations
            .first()
            .map(|alloc| alloc.working_dir.clone())
            .unwrap_or_else(get_current_dir);

        Box::pin(async move {
            let mut result = Map::with_capacity(allocation_ids.len());
            for allocation_id in allocation_ids {
                let status = get_allocation_status(&allocation_id, &workdir).await;
                result.insert(allocation_id, status);
            }

            Ok(result)
        })
    }

    fn remove_allocation(
        &self,
        allocation: &Allocation,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>> {
        let allocation_id = allocation.id.clone();
        let workdir = allocation.working_dir.clone();

        Box::pin(async move {
            let arguments = vec!["scancel", &allocation_id];
            log::debug!("Running Slurm command `{}`", arguments.join(" "));
            let mut command = create_command(arguments, &workdir);
            let output = command.output().await?;
            common::check_command_output(output)?;
            Ok(())
        })
    }
}

async fn get_allocation_status(
    allocation_id: &str,
    workdir: &Path,
) -> AutoAllocResult<AllocationExternalStatus> {
    let arguments = vec!["scontrol", "show", "job", allocation_id];
    log::debug!("Running Slurm command `{}`", arguments.join(" "));

    let mut command = create_command(arguments, workdir);
    let output = command.output().await.context("scontrol start failed")?;
    let output = common::check_command_output(output).context("scontrol execution failed")?;

    let output = output
        .stdout
        .to_str()
        .map_err(|err| anyhow::anyhow!("Invalid UTF-8 in scontrol output: {:?}", err))?;
    let items = get_scontrol_items(output);
    parse_slurm_status(items)
}

fn parse_slurm_status(items: Map<&str, &str>) -> AutoAllocResult<AllocationExternalStatus> {
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
        "PENDING" | "CONFIGURING" => AllocationExternalStatus::Queued,
        "RUNNING" | "COMPLETING" => AllocationExternalStatus::Running,
        "COMPLETED" | "CANCELLED" | "FAILED" | "TIMEOUT" => {
            let finished = matches!(status, "COMPLETED" | "TIMEOUT");
            let started_at = parse_time(get_key("StartTime")?)?;
            let finished_at = parse_time(get_key("EndTime")?)?;

            if finished {
                AllocationExternalStatus::Finished {
                    // TODO: handle case where the allocation didn't start at all
                    started_at: Some(started_at),
                    finished_at,
                }
            } else {
                AllocationExternalStatus::Failed {
                    started_at: Some(started_at),
                    finished_at,
                }
            }
        }
        _ => anyhow::bail!("Unknown Slurm job status {}", status),
    };
    Ok(status)
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
        writeln!(script, "#SBATCH {}", sbatch_args).unwrap();
    }

    let prefix = if nodes > 1 { "srun --overlap " } else { "" };
    write!(script, "\n{prefix}{worker_cmd}").unwrap();
    script
}
