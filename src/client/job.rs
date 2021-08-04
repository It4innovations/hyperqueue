use std::collections::BTreeSet;
use std::fmt::Write;

use cli_table::format::Justify;
use cli_table::{print_stdout, Cell, CellStruct, Color, Style, Table};
use colored::Colorize;
use tako::messages::common::StdioDef;

use crate::client::globalsettings::GlobalSettings;
use crate::client::resources::cpu_request_to_string;
use crate::client::status::{job_status, status_cell, task_status};
use crate::client::utils;
use crate::common::env::is_hq_env;
use crate::rpc_call;
use crate::server::job::{JobTaskCounters, JobTaskInfo, JobTaskState};
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobDetail, JobInfo, JobType, ToClientMessage};
use crate::{JobTaskCount, Map, WorkerId};

/// Maps worker IDs to hostnames.
type WorkerMap = Map<WorkerId, String>;

pub async fn get_worker_map(connection: &mut ClientConnection) -> anyhow::Result<WorkerMap> {
    let message = FromClientMessage::WorkerList;
    let response =
        rpc_call!(connection, message, ToClientMessage::WorkerListResponse(r) => r).await?;
    let map = response
        .workers
        .into_iter()
        .map(|w| (w.id, w.configuration.hostname))
        .collect();
    Ok(map)
}

fn job_status_with_counts_cells(info: &JobInfo) -> String {
    let row = |result: &mut String, string, value, color| {
        if value > 0 {
            let text = format!("{} ({})", string, value).color(color);
            writeln!(result, "{}", text).unwrap();
        }
    };
    let mut result = format!(
        "{}\n",
        utils::job_progress_bar(info.counters, info.n_tasks, 40)
    );
    row(
        &mut result,
        "RUNNING",
        info.counters.n_running_tasks,
        colored::Color::Yellow,
    );
    row(
        &mut result,
        "FAILED",
        info.counters.n_failed_tasks,
        colored::Color::Red,
    );
    row(
        &mut result,
        "FINISHED",
        info.counters.n_finished_tasks,
        colored::Color::Green,
    );
    row(
        &mut result,
        "CANCELED",
        info.counters.n_canceled_tasks,
        colored::Color::Magenta,
    );
    row(
        &mut result,
        "WAITING",
        info.counters.n_waiting_tasks(info.n_tasks),
        colored::Color::Cyan,
    );
    result
}

pub fn print_job_list(gsettings: &GlobalSettings, tasks: Vec<JobInfo>) {
    let rows: Vec<_> = tasks
        .into_iter()
        .map(|t| {
            let status = status_cell(job_status(&t));
            vec![
                t.id.cell().justify(Justify::Right),
                t.name.cell(),
                status,
                t.n_tasks.cell(),
            ]
        })
        .collect();

    let table = rows
        .table()
        .color_choice(gsettings.color_policy())
        .title(vec![
            "Id".cell().bold(true),
            "Name".cell().bold(true),
            "State".cell().bold(true),
            "Tasks".cell().bold(true),
        ]);
    assert!(print_stdout(table).is_ok());
}

pub fn stdio_to_cell(stdio: &StdioDef) -> CellStruct {
    match stdio {
        StdioDef::Null => "<None>".cell(),
        StdioDef::File(filename) => filename.display().cell(),
        StdioDef::Pipe => "<Stream>".cell(),
    }
}

pub fn print_job_detail(
    gsettings: &GlobalSettings,
    job: JobDetail,
    just_submitted: bool,
    show_tasks: bool,
    worker_map: WorkerMap,
) {
    let mut rows = vec![
        vec!["Id".cell().bold(true), job.info.id.cell()],
        vec!["Name".cell().bold(true), job.info.name.as_str().cell()],
    ];

    let status = if just_submitted {
        "SUBMITTED".cell().foreground_color(Some(Color::Cyan))
    } else if job.info.n_tasks == 1 {
        status_cell(job_status(&job.info))
    } else {
        job_status_with_counts_cells(&job.info).cell()
    };

    let state_label = "State".cell().bold(true);
    rows.push(vec![state_label, status]);

    let mut n_tasks = job.info.n_tasks.to_string();
    if let JobType::Array(array_def) = &job.job_type {
        n_tasks.push_str(&format!("; Ids: {}", array_def));
    }

    rows.push(vec!["Tasks".cell().bold(true), n_tasks.cell()]);
    rows.push(vec![
        "Workers".cell().bold(true),
        format_job_workers(&job, &worker_map).cell(),
    ]);

    let resources = cpu_request_to_string(job.resources.cpus());

    rows.push(vec![
        "Resources".cell().bold(true),
        if job.pin {
            format!("{} [pin]", resources)
        } else {
            resources
        }
        .cell(),
    ]);

    rows.push(vec!["Priority".cell().bold(true), job.priority.cell()]);

    let program_def = job.program_def;
    rows.push(vec![
        "Command".cell().bold(true),
        program_def
            .args
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join("\n")
            .cell(),
    ]);
    rows.push(vec![
        "Stdout".cell().bold(true),
        stdio_to_cell(&program_def.stdout),
    ]);
    rows.push(vec![
        "Stderr".cell().bold(true),
        stdio_to_cell(&program_def.stderr),
    ]);
    let mut env_vars: Vec<(_, _)> = program_def
        .env
        .iter()
        .filter(|(k, _)| !is_hq_env(k))
        .collect();
    env_vars.sort_by_key(|item| item.0);
    rows.push(vec![
        "Environment".cell().bold(true),
        env_vars
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("\n")
            .cell(),
    ]);

    rows.push(vec![
        "Working Dir".cell().bold(true),
        program_def
            .cwd
            .map(|cwd| cwd.display().to_string())
            .unwrap()
            .cell(),
    ]);

    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());

    if !job.tasks.is_empty() {
        print_job_tasks(
            gsettings,
            job.tasks,
            show_tasks,
            &job.info.counters,
            &worker_map,
        );
    }
}

const MAX_DISPLAYED_WORKERS: usize = 2;

fn format_job_workers(job: &JobDetail, worker_map: &WorkerMap) -> String {
    // BTreeSet is used to both filter duplicates and keep a stable order
    let worker_set: BTreeSet<_> = job
        .tasks
        .iter()
        .filter_map(|task| task.state.get_worker())
        .collect();
    let worker_count = worker_set.len();

    let mut result = worker_set
        .into_iter()
        .take(MAX_DISPLAYED_WORKERS)
        .map(|id| format_worker(id, worker_map))
        .collect::<Vec<_>>()
        .join(", ");

    if worker_count > MAX_DISPLAYED_WORKERS {
        write!(result, ", … ({} total)", worker_count).unwrap();
    }

    result
}

fn format_worker(id: WorkerId, worker_map: &WorkerMap) -> &str {
    worker_map
        .get(&id)
        .map(|s| s.as_str())
        .unwrap_or_else(|| "N/A")
}

fn print_job_tasks(
    gsettings: &GlobalSettings,
    mut tasks: Vec<JobTaskInfo>,
    show_tasks: bool,
    counters: &JobTaskCounters,
    worker_map: &WorkerMap,
) {
    tasks.sort_unstable_by_key(|t| t.task_id);

    let make_error_row = |t: &JobTaskInfo| match &t.state {
        JobTaskState::Failed { worker, error } => Some(vec![
            t.task_id.cell(),
            format_worker(*worker, worker_map).cell(),
            error.to_owned().cell().foreground_color(Some(Color::Red)),
        ]),
        _ => None,
    };

    if show_tasks {
        let rows: Vec<_> = tasks
            .iter()
            .map(|t| {
                vec![
                    t.task_id.cell(),
                    status_cell(task_status(&t.state)),
                    match t.state.get_worker() {
                        Some(worker) => format_worker(worker, worker_map),
                        _ => "",
                    }
                    .cell(),
                    match &t.state {
                        JobTaskState::Failed { error, .. } => {
                            error.to_owned().cell().foreground_color(Some(Color::Red))
                        }
                        _ => "".cell(),
                    },
                ]
            })
            .collect();
        let table = rows
            .table()
            .color_choice(gsettings.color_policy())
            .title(vec![
                "Task Id".cell().bold(true),
                "State".cell().bold(true),
                "Worker".cell().bold(true),
                "Message".cell().bold(true),
            ]);
        assert!(print_stdout(table).is_ok());
    } else {
        const SHOWN_TASKS: usize = 5;
        let fail_rows: Vec<_> = tasks
            .iter()
            .filter_map(make_error_row)
            .take(SHOWN_TASKS)
            .collect();

        if !fail_rows.is_empty() {
            let count = fail_rows.len() as JobTaskCount;
            let table = fail_rows
                .table()
                .color_choice(gsettings.color_policy())
                .title(vec![
                    "Task Id".cell().bold(true),
                    "Worker".cell().bold(true),
                    "Error".cell().bold(true),
                ]);
            assert!(print_stdout(table).is_ok());

            if count < counters.n_failed_tasks {
                println!(
                    "{} tasks failed. ({} shown)",
                    counters.n_failed_tasks, count
                );
            } else {
                println!("{} tasks failed.", counters.n_failed_tasks);
            }
        }
    }
}
