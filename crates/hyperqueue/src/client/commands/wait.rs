use std::collections::BTreeSet;
use std::io::Write;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::cli::{
    TASK_COLOR_CANCELED, TASK_COLOR_FAILED, TASK_COLOR_FINISHED, TASK_COLOR_RUNNING,
    job_progress_bar,
};
use crate::client::status::{Status, is_terminated};
use crate::common::arraydef::IntArray;
use crate::common::utils::str::pluralize;
use crate::server::job::JobTaskCounters;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, JobDetailRequest, JobInfo, JobInfoRequest, TaskIdSelector,
    TaskSelector, TaskStatusSelector, ToClientMessage, WaitForJobsRequest,
};
use crate::{JobId, JobTaskCount, rpc_call};
use colored::Colorize;

pub async fn wait_for_jobs(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    selector: IdSelector,
    wait_for_close: bool,
) -> anyhow::Result<()> {
    let start = SystemTime::now();
    let response = rpc_call!(
        session.connection(),
        FromClientMessage::WaitForJobs(WaitForJobsRequest {
            selector: selector.clone(),
            wait_for_close,
        }),
        ToClientMessage::WaitForJobsResponse(r) => r
    )
    .await?;

    let detail = match response.failed > 0 {
        false => vec![],
        true => {
            rpc_call!(
                session.connection(),
                FromClientMessage::JobDetail(JobDetailRequest {
                    job_id_selector: selector,
                    task_selector: Some(TaskSelector {
                        id_selector: TaskIdSelector::All,
                        status_selector: TaskStatusSelector::Specific(vec![Status::Failed]),
                    })
                }),
                ToClientMessage::JobDetailResponse(r) => r.details
            )
            .await?
        }
    };

    let duration = start.elapsed()?;
    gsettings.printer().print_job_wait(
        duration,
        &response,
        &detail,
        get_worker_map(session).await?,
    );

    if response.failed > 0 || response.canceled > 0 {
        return Err(anyhow::anyhow!(
            "Some jobs have failed or have been canceled"
        ));
    }

    Ok(())
}

pub async fn wait_for_jobs_with_progress(
    session: &mut ClientSession,
    jobs: &[JobInfo],
) -> anyhow::Result<()> {
    if jobs.iter().all(is_terminated) {
        log::warn!("There are no jobs to wait for");
    } else {
        let mut unfinished_job_ids: BTreeSet<JobId> = jobs
            .iter()
            .filter(|info| !is_terminated(info))
            .map(|info| info.id)
            .collect();
        let unfinished_tasks = jobs
            .iter()
            .filter(|info| !is_terminated(info))
            .map(|info| info.n_tasks)
            .sum::<u32>();

        let initial_unfinished_jobs = unfinished_job_ids.len();
        log::info!(
            "Waiting for {} {} with {} {}",
            initial_unfinished_jobs,
            pluralize("job", initial_unfinished_jobs),
            unfinished_tasks,
            pluralize("task", unfinished_tasks as usize),
        );

        let total_tasks: JobTaskCount = jobs.iter().map(|info| info.n_tasks).sum();

        // Counters of jobs that have all been finished
        // Note: this ignores the fact that some jobs might be open
        let mut counters_finished = jobs
            .iter()
            .filter(|info| is_terminated(info))
            .map(|info| info.counters)
            .fold(JobTaskCounters::default(), |acc, c| acc + c);

        loop {
            // Only ask for status of unfinished jobs
            let response = rpc_call!(
                session.connection(),
                FromClientMessage::JobInfo(JobInfoRequest {
                    selector: IdSelector::Specific(IntArray::from_sorted_ids(unfinished_job_ids.iter().map(|x| x.as_num()))),
                }),
                ToClientMessage::JobInfoResponse(r) => r
            )
            .await?;

            let mut current_counters = counters_finished;
            for job in &response.jobs {
                current_counters = current_counters + job.counters;

                if is_terminated(job) {
                    unfinished_job_ids.remove(&job.id);
                    counters_finished = counters_finished + job.counters;
                }
            }

            let completed_jobs = jobs.len() - unfinished_job_ids.len();
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
                jobs.len(),
                completed_tasks,
                total_tasks,
                status
            );
            std::io::stdout().flush().unwrap();

            if unfinished_job_ids.is_empty() {
                // Move the cursor to a new line
                println!();
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }

        if counters_finished.n_failed_tasks > 0 {
            anyhow::bail!("Some jobs have failed");
        }
        if counters_finished.n_canceled_tasks > 0 {
            anyhow::bail!("Some jobs were canceled");
        }
    }
    Ok(())
}
