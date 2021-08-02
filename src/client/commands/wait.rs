use crate::client::status::is_terminated;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobInfoRequest, JobSelector, ToClientMessage};
use crate::{rpc_call, JobId, JobTaskCount, Set};

use anyhow::bail;
use colored::Colorize;
use std::fmt::{self, Display};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone, Copy)]
struct DisplayRepeat<T>(usize, T);

impl<T: Display> Display for DisplayRepeat<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for _ in 0..self.0 {
            self.1.fmt(f)?;
        }
        Ok(())
    }
}

fn repeat<T>(times: usize, item: T) -> DisplayRepeat<T> {
    DisplayRepeat(times, item)
}

pub async fn wait_on_job(
    connection: &mut ClientConnection,
    selector: JobSelector,
) -> anyhow::Result<()> {
    let width: u32 = 25;
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
        let total_jobs: u32 = job_ids.len() as u32;

        log::info!(
            "Waiting for {} job(s) with a {} task(s)",
            total_jobs,
            total_tasks
        );

        let mut non_terminated_ids: Set<JobId> = job_ids.into_iter().collect();
        let (mut current_jobs, mut finished, mut failed, mut canceled, mut running) =
            (0, 0, 0, 0, 0);

        loop {
            let ids_ref = &mut non_terminated_ids;
            let response = rpc_call!(
                connection,
                FromClientMessage::JobInfo(JobInfoRequest {
                    selector: JobSelector::Specific(ids_ref.iter().copied().collect()),
                }),
                ToClientMessage::JobInfoResponse(r) => r
            )
            .await?;

            let (mut tmp_finished, mut tmp_failed, mut tmp_canceled, mut tmp_running) =
                (0, 0, 0, 0);
            for job in &response.jobs {
                if is_terminated(&job) {
                    non_terminated_ids.remove(&job.id);
                    current_jobs += 1;

                    finished += job.counters.n_finished_tasks;
                    canceled += job.counters.n_canceled_tasks;
                    failed += job.counters.n_failed_tasks;
                    running += job.counters.n_running_tasks;
                } else {
                    tmp_finished += job.counters.n_finished_tasks;
                    tmp_canceled += job.counters.n_canceled_tasks;
                    tmp_failed += job.counters.n_failed_tasks;
                    tmp_running += job.counters.n_running_tasks;
                }
            }

            let jobs_percentage = current_jobs / total_jobs;
            let current_tasks =
                finished + tmp_finished + canceled + tmp_canceled + failed + tmp_failed;
            let tasks_percentage = current_tasks / total_tasks;

            println!(
                "[{}>{}] {} / {} jobs\n\
                [{}>{}] {} / {} tasks ({} {} | {} {} | {} {} | {} {})",
                repeat((width * jobs_percentage) as usize, '#'),
                repeat((width * (1 - jobs_percentage)) as usize, '-'),
                current_jobs,
                total_jobs,
                repeat((width * tasks_percentage) as usize, '#'),
                repeat((width * (1 - tasks_percentage)) as usize, '-'),
                current_tasks,
                total_tasks,
                "RUNNING".yellow(),
                running + tmp_running,
                "FINISHED".green(),
                finished + tmp_finished,
                "CANCELED".magenta(),
                canceled + tmp_canceled,
                "FAILED".red(),
                failed + tmp_failed,
            );

            if non_terminated_ids.is_empty() {
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }

        if failed > 0 {
            bail!("Some jobs have failed");
        }
        if canceled > 0 {
            bail!("Some jobs were canceled");
        }
    }
    Ok(())
}
