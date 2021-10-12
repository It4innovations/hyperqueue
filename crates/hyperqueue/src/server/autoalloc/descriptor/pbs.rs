use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use crate::common::manager::info::ManagerType;
use anyhow::Context;
use bstr::ByteSlice;
use tokio::process::Command;

use crate::common::manager::pbs::{format_pbs_duration, parse_pbs_datetime};
use crate::common::timeutils::local_to_system_time;
use crate::server::autoalloc::descriptor::common::{
    build_worker_args, check_command_output, create_allocation_dir, JOBID_FILE_NAME,
    SUBMIT_SCRIPT_NAME,
};
use crate::server::autoalloc::descriptor::{CreatedAllocation, QueueHandler};
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::{AutoAllocResult, DescriptorId, QueueInfo};

pub struct PbsHandler {
    server_directory: PathBuf,
    hq_path: PathBuf,
    qsub_args: Vec<String>,
}

impl PbsHandler {
    pub async fn new(server_directory: PathBuf, qsub_args: Vec<String>) -> anyhow::Result<Self> {
        let hq_path = std::env::current_exe().context("Cannot get HyperQueue path")?;
        Ok(Self {
            server_directory,
            hq_path,
            qsub_args,
        })
    }
}

impl QueueHandler for PbsHandler {
    fn schedule_allocation(
        &self,
        descriptor_id: DescriptorId,
        queue_info: &QueueInfo,
        worker_count: u64,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<CreatedAllocation>>>> {
        let queue = queue_info.queue.clone();
        let timelimit = queue_info.timelimit;
        let hq_path = self.hq_path.clone();
        let server_directory = self.server_directory.clone();
        let name = descriptor_id.to_string();
        let qsub_args = self.qsub_args.clone();

        Box::pin(async move {
            let directory = create_allocation_dir(server_directory.clone(), &name)?;
            let script_path = directory.join(SUBMIT_SCRIPT_NAME);
            let script_path = script_path.to_str().unwrap();

            let worker_args = build_worker_args(&hq_path, ManagerType::Pbs, &server_directory);

            let script = build_pbs_submit_script(
                worker_count,
                timelimit.as_ref(),
                &queue,
                &format!("hq-alloc-{}", descriptor_id),
                &directory.join("stdout").display().to_string(),
                &directory.join("stderr").display().to_string(),
                &qsub_args.join(" "),
                &worker_args,
            );
            std::fs::write(&script_path, script)
                .with_context(|| anyhow::anyhow!("Cannot write PBS script into {}", script_path))?;

            let arguments = vec!["qsub", script_path];

            log::debug!("Running PBS command `{}`", arguments.join(" "));
            let mut command = Command::new(&arguments[0]);
            command.args(&arguments[1..]);

            let output = command.output().await.context("qsub start failed")?;
            let output = check_command_output(output).context("qsub execution failed")?;

            let job_id = output
                .stdout
                .to_str()
                .map_err(|e| anyhow::anyhow!("Invalid UTF-8 qsub output: {:?}", e))?
                .trim();
            let job_id = job_id.to_string();

            // Write the PBS job id to the folder as a debug information
            std::fs::write(directory.join(JOBID_FILE_NAME), &job_id)?;

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
            // -x will also display finished jobs
            let arguments = vec!["qstat", "-f", &allocation_id, "-F", "json", "-x"];
            log::debug!("Running PBS command `{}`", arguments.join(" "));

            let mut command = Command::new(arguments[0]);
            command.args(&arguments[1..]);

            let output = command.output().await.context("qstat start failed")?;
            let output = check_command_output(output).context("qstat execution failed")?;
            let data: serde_json::Value =
                serde_json::from_slice(&output.stdout).context("Cannot parse qstat JSON output")?;
            let job = &data["Jobs"][&allocation_id];

            // Job was not found
            if job.is_null() {
                return Ok(None);
            }

            let state = get_json_str(&job["job_state"], "Job state")?;
            let start_time_key = "stime";
            let modification_time_key = "mtime";

            let parse_time = |key: &str| {
                let value = &job[key];
                value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing time key {} in PBS", key))
                    .and_then(|v| AutoAllocResult::Ok(local_to_system_time(parse_pbs_datetime(v)?)))
            };

            let status = match state {
                "Q" => AllocationStatus::Queued,
                "R" => AllocationStatus::Running {
                    started_at: parse_time(start_time_key)?,
                },
                "F" => {
                    let exit_status = get_json_number(&job["Exit_status"], "Exit status")?;
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

            Ok(Some(status))
        })
    }

    fn remove_allocation(
        &self,
        allocation_id: AllocationId,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>> {
        Box::pin(async move {
            let arguments = vec!["qdel".to_string(), allocation_id];
            log::debug!("Running PBS command `{}`", arguments.join(" "));
            let mut command = Command::new(&arguments[0]);
            command.args(&arguments[1..]);

            let output = command.output().await?;
            check_command_output(output)?;
            Ok(())
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn build_pbs_submit_script(
    nodes: u64,
    timelimit: Option<&Duration>,
    queue: &str,
    name: &str,
    stdout: &str,
    stderr: &str,
    qsub_args: &str,
    worker_cmd: &str,
) -> String {
    let mut script = format!(
        r##"#!/bin/bash
#PBS -l select={nodes}
#PBS -q {queue}
#PBS -N {name}
#PBS -o {stdout}
#PBS -e {stderr}
"##,
        nodes = nodes,
        queue = queue,
        name = name,
        stdout = stdout,
        stderr = stderr,
    );

    if let Some(timelimit) = timelimit {
        script.push_str(&format!(
            "#PBS -l walltime={}\n",
            format_pbs_duration(timelimit)
        ));
    }

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
