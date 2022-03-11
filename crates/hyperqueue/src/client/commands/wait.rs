use std::io::Write;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::cli::{
    job_progress_bar, TASK_COLOR_CANCELED, TASK_COLOR_FAILED, TASK_COLOR_FINISHED,
    TASK_COLOR_RUNNING,
};
use crate::client::status::{is_terminated, Status};
use crate::common::arraydef::IntArray;
use crate::common::utils::str::pluralize;
use crate::server::job::JobTaskCounters;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, JobDetailRequest, JobInfo, JobInfoRequest, TaskIdSelector,
    TaskSelector, TaskStatusSelector, ToClientMessage, WaitForJobsRequest,
};
use crate::{rpc_call, JobId, JobTaskCount, Set};
use colored::Colorize;

pub async fn wait_for_jobs(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    selector: IdSelector,
) -> anyhow::Result<()> {
    let start = SystemTime::now();
    let response = rpc_call!(
        connection,
        FromClientMessage::WaitForJobs(WaitForJobsRequest {
            selector: selector.clone(),
        }),
        ToClientMessage::WaitForJobsResponse(r) => r
    )
    .await?;

    let detail = match response.failed > 0 {
        false => vec![],
        true => {
            rpc_call!(
                connection,
                FromClientMessage::JobDetail(JobDetailRequest {
                    job_id_selector: selector,
                    task_selector: Some(TaskSelector {
                        id_selector: TaskIdSelector::All,
                        status_selector: TaskStatusSelector::Specific(vec![Status::Failed]),
                    })
                }),
                ToClientMessage::JobDetailResponse(r) => r
            )
            .await?
        }
    };

    let duration = start.elapsed()?;
    gsettings.printer().print_job_wait(
        duration,
        &response,
        &detail,
        get_worker_map(connection).await?,
    );

    if response.failed > 0 || response.canceled > 0 {
        return Err(anyhow::anyhow!(
            "Some jobs have failed or have been canceled"
        ));
    }

    Ok(())
}

pub async fn wait_for_jobs_with_progress(
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
            "Waiting for {} {} with {} {}",
            total_jobs,
            pluralize("job", total_jobs),
            total_tasks,
            pluralize("task", total_tasks as usize),
        );

        let mut counters = JobTaskCounters::default();

        loop {
            let ids_ref = &mut remaining_job_ids;
            let response = rpc_call!(
                connection,
                FromClientMessage::JobInfo(JobInfoRequest {
                    selector: IdSelector::Specific(IntArray::from_ids(ids_ref.iter().map(|&id| id.into()).collect())),
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
            anyhow::bail!("Some jobs have failed");
        }
        if counters.n_canceled_tasks > 0 {
            anyhow::bail!("Some jobs were canceled");
        }
    }
    Ok(())
}
