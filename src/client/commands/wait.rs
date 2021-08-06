use crate::client::status::is_terminated;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, JobInfo, JobInfoRequest, Selector, ToClientMessage,
};
use crate::{rpc_call, JobId, JobTaskCount, Set};

use crate::client::utils::{
    job_progress_bar, TASK_COLOR_CANCELED, TASK_COLOR_FAILED, TASK_COLOR_FINISHED,
    TASK_COLOR_RUNNING,
};
use crate::common::arraydef::IntArray;
use crate::server::job::JobTaskCounters;
use anyhow::bail;
use colored::Colorize;
use std::io::Write;
use std::time::Duration;
use tokio::time::sleep;

pub async fn wait_for_job_with_info(
    connection: &mut ClientConnection,
    job_info: JobInfo,
) -> anyhow::Result<()> {
    wait_for_jobs(connection, vec![job_info]).await
}

pub async fn wait_for_job_with_selector(
    connection: &mut ClientConnection,
    selector: Selector,
) -> anyhow::Result<()> {
    let response = rpc_call!(
        connection,
        FromClientMessage::JobInfo(JobInfoRequest {
            selector,
        }),
        ToClientMessage::JobInfoResponse(r) => r
    )
    .await?;

    wait_for_jobs(connection, response.jobs).await
}

async fn wait_for_jobs(
    connection: &mut ClientConnection,
    mut jobs: Vec<JobInfo>,
) -> anyhow::Result<()> {
    jobs.retain(|info| !is_terminated(info));

    if jobs.is_empty() {
        log::warn!("There are no jobs to wait for");
    } else {
        let total_tasks: JobTaskCount = jobs.iter().map(|info| info.n_tasks).sum();
        let mut remaining_job_ids: Set<JobId> = jobs.into_iter().map(|info| info.id).collect();

        let total_jobs = remaining_job_ids.len();

        log::info!(
            "Waiting for {} job(s) with a {} task(s)",
            total_jobs,
            total_tasks
        );

        let mut counters = JobTaskCounters::default();

        loop {
            let ids_ref = &mut remaining_job_ids;
            let response = rpc_call!(
                connection,
                FromClientMessage::JobInfo(JobInfoRequest {
                    selector: Selector::Specific(IntArray::from_ids(ids_ref.iter().copied().collect())),
                }),
                ToClientMessage::JobInfoResponse(r) => r
            )
            .await?;

            let mut current_counters = counters;
            for job in &response.jobs {
                current_counters = current_counters + job.counters;

                if is_terminated(job) {
                    remaining_job_ids.remove(&job.id);
                    counters = counters + job.counters;
                }
            }

            let completed_jobs = total_jobs - remaining_job_ids.len();
            let completed_tasks = current_counters.n_finished_tasks
                + current_counters.n_canceled_tasks
                + current_counters.n_failed_tasks;

            let mut statuses = vec![];
            let mut add_count = |count, name: &str, color| {
                if count > 0 {
                    statuses.push(format!("{} {}", count, name.to_string().color(color)));
                }
            };
            add_count(
                current_counters.n_running_tasks,
                "RUNNING",
                TASK_COLOR_RUNNING,
            );
            add_count(
                current_counters.n_finished_tasks,
                "FINISHED",
                TASK_COLOR_FINISHED,
            );
            add_count(current_counters.n_failed_tasks, "FAILED", TASK_COLOR_FAILED);
            add_count(
                current_counters.n_canceled_tasks,
                "CANCELED",
                TASK_COLOR_CANCELED,
            );
            let status = if !statuses.is_empty() {
                format!("({})", statuses.join(", "))
            } else {
                "".to_string()
            };

            // \x1b[2K clears the line
            print!(
                "\r\x1b[2K{} {}/{} jobs, {}/{} tasks {}",
                job_progress_bar(current_counters, total_tasks, 40),
                completed_jobs,
                total_jobs,
                completed_tasks,
                total_tasks,
                status
            );
            std::io::stdout().flush().unwrap();

            if remaining_job_ids.is_empty() {
                // Move the cursor to a new line
                println!();
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }

        if counters.n_failed_tasks > 0 {
            bail!("Some jobs have failed");
        }
        if counters.n_canceled_tasks > 0 {
            bail!("Some jobs were canceled");
        }
    }
    Ok(())
}
