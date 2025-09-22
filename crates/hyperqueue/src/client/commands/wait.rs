use std::ffi::OsStr;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::process::Command;

use crate::client::output::cli::{
    TASK_COLOR_CANCELED, TASK_COLOR_FAILED, TASK_COLOR_FINISHED, TASK_COLOR_RUNNING,
    job_progress_bar,
};
use crate::client::status::is_terminated;
use crate::common::utils::str::pluralize;
use crate::server::event::payload::{EventPayload, TaskNotification};
use crate::server::job::JobTaskCounters;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{JobInfo, ToClientMessage};
use colored::Colorize;
use tako::{Set, TaskId};

fn process_on_notify(program: &str, args: &[String], notification: &TaskNotification) {
    log::info!("Running on_notify callback: {program} {args:?}: {notification:?}");
    let mut child = match Command::new(program)
        .args(args)
        .arg(OsStr::from_bytes(&notification.message))
        .env("HQ_JOB_ID", notification.task_id.job_id().to_string())
        .env("HQ_TASK_ID", notification.task_id.job_task_id().to_string())
        .env("HQ_WORKER_ID", notification.worker_id.to_string())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            log::warn!("Failed to run on_notify callback: {e}");
            return;
        }
    };
    match child.wait() {
        Ok(s) => {
            if !s.success() {
                log::warn!("on_notify callback finished with exit code: {s}");
            }
        }
        Err(e) => {
            log::warn!("Failed to run on_notify callback: {e}");
        }
    }
}

pub async fn wait_for_jobs(
    session: &mut ClientSession,
    jobs: &[JobInfo],
    wait_for_close: bool,
    on_notify: Option<&str>,
) -> anyhow::Result<()> {
    let mut unfinished_jobs = Set::new();
    for job in jobs {
        if !is_terminated(job) || (wait_for_close && job.is_open) {
            unfinished_jobs.insert(job.id);
        }
    }
    let on_notify_program_and_args =
        on_notify.map(|s| shlex::split(s).unwrap_or_else(|| vec![s.to_string()]));
    while !unfinished_jobs.is_empty() {
        if let Some(msg) = session.connection().receive().await {
            let msg = msg?;
            let job_id = match &msg {
                ToClientMessage::Event(event) => match &event.payload {
                    EventPayload::JobCompleted(job_id) => job_id,
                    EventPayload::JobIdle(job_id) if !wait_for_close => job_id,
                    EventPayload::TaskNotify(notification) => {
                        let program_and_args = on_notify_program_and_args.as_ref().unwrap();
                        process_on_notify(
                            &program_and_args[0],
                            &program_and_args[1..],
                            notification,
                        );
                        continue;
                    }
                    _ => continue,
                },
                _ => {
                    return Err(anyhow::anyhow!("Unexpected message from server"));
                }
            };
            unfinished_jobs.remove(job_id);
        } else {
            return Ok(());
        }
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

    let unfinished_tasks = counters.n_running_tasks + counters.n_waiting_tasks(n_tasks);
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
