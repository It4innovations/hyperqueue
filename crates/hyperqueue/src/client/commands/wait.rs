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
use crate::rpc_call;
use crate::server::event::payload::EventPayload;
use crate::server::job::JobTaskCounters;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, JobDetailRequest, JobInfo, JobInfoRequest, TaskIdSelector,
    TaskSelector, TaskStatusSelector, ToClientMessage, WaitForJobsRequest,
};
use colored::Colorize;
use tako::{JobId, JobTaskCount, TaskId};

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
    let mut n_tasks = 0;
    let mut counters = JobTaskCounters::default();
    let mut completed_jobs = 0;

    let mut running_tasks: tako::Set<TaskId> = Default::default();

    for job in jobs {
        n_tasks += job.n_tasks;
        counters = counters + job.counters;
        if is_terminated(job) {
            completed_jobs += 1;
        }
        for job_task_id in &job.running_tasks {
            running_tasks.insert(TaskId::new(job.id, *job_task_id));
        }
    }

    let mut unfinished_tasks = counters.n_running_tasks + counters.n_waiting_tasks(n_tasks);
    if unfinished_tasks == 0 {
        log::warn!("There are no jobs to wait for");
        return Ok(());
    }

    log::info!(
        "Waiting for {} {} with {} {}",
        jobs.len(),
        pluralize("job", jobs.len()),
        unfinished_tasks,
        pluralize("task", unfinished_tasks as usize),
    );
    let mut status = String::new();
    loop {
        status.clear();
        let mut add_count = |count, name: &str, color| {
            use std::fmt::Write;
            if count > 0 {
                write!(
                    status,
                    "{}{} {}",
                    if status.is_empty() { "" } else { " " },
                    count,
                    name.color(color)
                )
                .unwrap();
            }
        };
        add_count(counters.n_running_tasks, "RUNNING", TASK_COLOR_RUNNING);
        add_count(counters.n_finished_tasks, "FINISHED", TASK_COLOR_FINISHED);
        add_count(counters.n_failed_tasks, "FAILED", TASK_COLOR_FAILED);
        add_count(counters.n_canceled_tasks, "CANCELED", TASK_COLOR_CANCELED);

        // \x1b[2K clears the line
        print!(
            "\r\x1b[2K{} {}/{} jobs, {}/{} tasks {}",
            job_progress_bar(counters, n_tasks, 40),
            completed_jobs,
            jobs.len(),
            counters.completed_tasks(),
            n_tasks,
            status
        );
        std::io::stdout().flush().unwrap();

        if completed_jobs >= jobs.len() {
            break;
        }

        if let Some(msg) = session.connection().receive().await {
            let msg = msg?;
            match &msg {
                ToClientMessage::Event(event) => match &event.payload {
                    EventPayload::JobCompleted(_) => completed_jobs += 1,
                    EventPayload::TaskStarted { task_id, .. } => {
                        counters.n_running_tasks += 1;
                        running_tasks.insert(*task_id);
                    }
                    EventPayload::TaskFinished { task_id } => {
                        if running_tasks.remove(task_id) {
                            counters.n_running_tasks -= 1;
                        }
                        counters.n_finished_tasks += 1;
                    }
                    EventPayload::TaskFailed { task_id, .. } => {
                        if running_tasks.remove(task_id) {
                            counters.n_running_tasks -= 1;
                        }
                        counters.n_failed_tasks += 1;
                    }
                    EventPayload::TasksCanceled { task_ids } => {
                        for task_id in task_ids {
                            if running_tasks.remove(task_id) {
                                counters.n_running_tasks -= 1;
                            }
                            counters.n_canceled_tasks += 1;
                        }
                    }
                    _ => {}
                },
                _ => {
                    log::warn!("Unexpected message from server: {:?}", &msg);
                }
            }
        } else {
            return Ok(());
        }
    }

    Ok(())
}
