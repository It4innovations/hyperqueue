use crate::client::status::is_terminated;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobInfoRequest, JobSelector, ToClientMessage};
use crate::{rpc_call, JobId, JobTaskCount, Set};

use crate::server::job::JobTaskCounters;
use anyhow::bail;
use colored::Colorize;
use std::time::Duration;
use tokio::time::sleep;

pub async fn wait_on_job(
    connection: &mut ClientConnection,
    selector: JobSelector,
) -> anyhow::Result<()> {
    let width: f64 = 25.0;
    let response = rpc_call!(
        connection,
        FromClientMessage::JobInfo(JobInfoRequest {
            selector,
        }),
        ToClientMessage::JobInfoResponse(r) => r
    )
    .await?;

    let job_ids: Vec<JobId> = response
        .jobs
        .iter()
        .filter(|info| !is_terminated(info))
        .map(|info| info.id)
        .collect();

    if job_ids.is_empty() {
        log::warn!("There are no jobs to wait for");
    } else {
        let total_tasks: JobTaskCount = response
            .jobs
            .iter()
            .filter(|info| !is_terminated(info))
            .map(|info| info.n_tasks)
            .sum();
        let total_jobs = job_ids.len();

        log::info!(
            "Waiting for {} job(s) with a {} task(s)",
            total_jobs,
            total_tasks
        );

        let mut remaining_job_ids: Set<JobId> = job_ids.into_iter().collect();
        let mut counters = JobTaskCounters::default();

        loop {
            let ids_ref = &mut remaining_job_ids;
            let response = rpc_call!(
                connection,
                FromClientMessage::JobInfo(JobInfoRequest {
                    selector: JobSelector::Specific(ids_ref.iter().copied().collect()),
                }),
                ToClientMessage::JobInfoResponse(r) => r
            )
            .await?;

            let mut current_counters = counters;
            for job in &response.jobs {
                current_counters = current_counters + job.counters;

                if is_terminated(&job) {
                    remaining_job_ids.remove(&job.id);
                    counters = counters + job.counters;
                }
            }

            let completed_jobs = total_jobs - remaining_job_ids.len();
            let jobs_percentage: f64 = completed_jobs as f64 / total_jobs as f64;
            let completed_tasks = current_counters.n_finished_tasks
                + current_counters.n_canceled_tasks
                + current_counters.n_failed_tasks;
            let tasks_percentage: f64 = completed_tasks as f64 / total_tasks as f64;

            let job_arrow = if completed_jobs < total_jobs { ">" } else { "" };
            let task_arrow = if completed_tasks < total_tasks {
                ">"
            } else {
                ""
            };

            println!(
                "[{}{job_arrow}{}] {} / {} jobs\n\
                [{}{task_arrow}{}] {} / {} tasks ({} {} | {} {} | {} {} | {} {})",
                "#".repeat((width * jobs_percentage) as usize),
                "-".repeat((width * (1.0 - jobs_percentage)) as usize),
                completed_jobs,
                total_jobs,
                "#".repeat((width * tasks_percentage) as usize),
                "-".repeat((width * (1.0 - tasks_percentage)) as usize),
                completed_tasks,
                total_tasks,
                "RUNNING".yellow(),
                current_counters.n_running_tasks,
                "FINISHED".green(),
                current_counters.n_finished_tasks,
                "CANCELED".magenta(),
                current_counters.n_canceled_tasks,
                "FAILED".red(),
                current_counters.n_failed_tasks,
                job_arrow = job_arrow,
                task_arrow = task_arrow
            );

            if remaining_job_ids.is_empty() {
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
