use crate::Map;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use crate::common::fsutils::get_current_dir;
use anyhow::Context;

use crate::common::manager::info::ManagerType;
use crate::common::manager::pbs::{format_pbs_duration, parse_pbs_datetime};
use crate::common::timeutils::local_to_system_time;
use crate::server::autoalloc::descriptor::common::{
    build_worker_args, check_command_output, create_allocation_dir, create_command, submit_script,
    ExternalHandler,
};
use crate::server::autoalloc::descriptor::{
    AllocationStatusMap, AllocationSubmissionResult, QueueHandler,
};
use crate::server::autoalloc::state::AllocationStatus;
use crate::server::autoalloc::{
    Allocation, AllocationId, AutoAllocResult, DescriptorId, QueueInfo,
};

pub struct PbsHandler {
    handler: ExternalHandler,
}

impl PbsHandler {
    pub fn new(server_directory: PathBuf, name: Option<String>) -> anyhow::Result<Self> {
        let handler = ExternalHandler::new(server_directory, name)?;
        Ok(Self { handler })
    }
}

impl QueueHandler for PbsHandler {
    fn submit_allocation(
        &mut self,
        descriptor_id: DescriptorId,
        queue_info: &QueueInfo,
        worker_count: u64,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationSubmissionResult>>>> {
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
                build_worker_args(&hq_path, ManagerType::Pbs, &server_directory, &queue_info);

            let script = build_pbs_submit_script(
                worker_count,
                timelimit,
                &format!("hq-alloc-{}", descriptor_id),
                &directory.join("stdout").display().to_string(),
                &directory.join("stderr").display().to_string(),
                &queue_info.additional_args.join(" "),
                &worker_args,
            );
            let job_id =
                submit_script(script, "qsub", &directory, |output| Ok(output.to_string())).await;

            Ok(AllocationSubmissionResult {
                id: job_id,
                working_dir: directory,
            })
        })
    }

    fn get_status_of_allocations(
        &self,
        allocations: &[&Allocation],
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationStatusMap>>>> {
        let mut arguments = vec!["qstat"];
        for &allocation in allocations {
            arguments.extend_from_slice(&["-f", &allocation.id]);
        }

        // -x will also display finished jobs
        arguments.extend_from_slice(&["-F", "json", "-x"]);

        let allocation_ids: Vec<AllocationId> =
            allocations.iter().map(|alloc| alloc.id.clone()).collect();
        let workdir = allocations
            .first()
            .map(|alloc| alloc.working_dir.clone())
            .unwrap_or_else(get_current_dir);

        log::debug!("Running PBS command `{}`", arguments.join(" "));

        let mut command = create_command(arguments, &workdir);

        Box::pin(async move {
            let output = command.output().await.context("qstat start failed")?;
            let output = check_command_output(output).context("qstat execution failed")?;

            let data: serde_json::Value =
                serde_json::from_slice(&output.stdout).context("Cannot parse qstat JSON output")?;

            let mut result = Map::with_capacity(allocation_ids.len());

            let jobs = &data["Jobs"];
            for allocation_id in allocation_ids {
                let allocation = &jobs[&allocation_id];
                if !allocation.is_null() {
                    let status = parse_allocation_status(allocation);
                    result.insert(allocation_id, status);
                }
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
            let arguments = vec!["qdel", &allocation_id];
            log::debug!("Running PBS command `{}`", arguments.join(" "));

            let mut command = create_command(arguments, &workdir);
            let output = command.output().await?;
            check_command_output(output)?;
            Ok(())
        })
    }
}

fn parse_allocation_status(allocation: &serde_json::Value) -> AutoAllocResult<AllocationStatus> {
    let state = get_json_str(&allocation["job_state"], "Job state")?;
    let start_time_key = "stime";
    let modification_time_key = "mtime";

    let parse_time = |key: &str| {
        let value = &allocation[key];
        value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing time key {} in PBS", key))
            .and_then(|v| AutoAllocResult::Ok(local_to_system_time(parse_pbs_datetime(v)?)))
    };

    let status = match state {
        "Q" => AllocationStatus::Queued,
        "R" | "E" => AllocationStatus::Running {
            started_at: parse_time(start_time_key)?,
        },
        "F" => {
            let exit_status = get_json_number(&allocation["Exit_status"], "Exit status")?;
            let started_at = parse_time(start_time_key)?;
            let finished_at = parse_time(modification_time_key)?;

            if exit_status == 0 {
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
        status => anyhow::bail!("Unknown PBS job status {}", status),
    };
    Ok(status)
}

#[allow(clippy::too_many_arguments)]
fn build_pbs_submit_script(
    nodes: u64,
    timelimit: Duration,
    name: &str,
    stdout: &str,
    stderr: &str,
    qsub_args: &str,
    worker_cmd: &str,
) -> String {
    let mut script = format!(
        r##"#!/bin/bash
#PBS -l select={nodes}
#PBS -N {name}
#PBS -o {stdout}
#PBS -e {stderr}
#PBS -l walltime={walltime}
"##,
        nodes = nodes,
        name = name,
        stdout = stdout,
        stderr = stderr,
        walltime = format_pbs_duration(&timelimit)
    );

    if !qsub_args.is_empty() {
        script.push_str(&format!("#PBS {}\n", qsub_args));
    }

    script.push_str(&format!("\n{}", worker_cmd));
    script
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
