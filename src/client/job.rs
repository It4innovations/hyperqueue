use cli_table::format::Justify;
use cli_table::{print_stdout, Cell, CellStruct, Color, Style, Table};

use crate::client::globalsettings::GlobalSettings;
use crate::transfer::messages::{JobInfo, JobDetail, JobType};
use crate::server::job::{JobTaskInfo, JobTaskState, JobTaskCounters};
use crate::JobTaskCount;


enum Status {
    Waiting,
    Running,
    Finished,
    Failed,
    Canceled,
}

fn job_status(info: &JobInfo) -> Status {
    if info.counters.n_running_tasks > 0 {
        return Status::Running;
    }
    if info.counters.n_canceled_tasks > 0 {
        return Status::Canceled;
    }
    if info.counters.n_failed_tasks > 0 {
        return Status::Failed;
    }
    if info.counters.n_finished_tasks == info.n_tasks {
        return Status::Finished;
    }
    return Status::Waiting;
}

fn task_status(status: &JobTaskState) -> Status {
    match status {
        JobTaskState::Waiting => Status::Waiting,
        JobTaskState::Running => Status::Running,
        JobTaskState::Finished => Status::Finished,
        JobTaskState::Failed(_) => Status::Failed,
        JobTaskState::Canceled => Status::Canceled,
    }
}

fn status_cell(status: Status) -> CellStruct {
    match status {
        Status::Waiting => "WAITING".cell().foreground_color(Some(Color::Cyan)),
        Status::Finished => "FINISHED".cell().foreground_color(Some(Color::Green)),
        Status::Failed => "FAILED".cell().foreground_color(Some(Color::Red)),
        Status::Running => "RUNNING".cell().foreground_color(Some(Color::Yellow)),
        Status::Canceled => "CANCELED".cell().foreground_color(Some(Color::Magenta)),
    }
}

fn job_status_with_counts_cells(info: &JobInfo) -> Vec<CellStruct> {
    let row = |result: &mut Vec<_>, string, value, color| {
        if value > 0 {
            result.push(format!("{} ({})", string, value).cell().foreground_color(Some(color)));
        }
    };
    let mut result : Vec<CellStruct> = Vec::new();
    row(&mut result, "RUNNING", info.counters.n_running_tasks, Color::Yellow);
    row(&mut result, "FAILED", info.counters.n_failed_tasks, Color::Red);
    row(&mut result, "FINISHED", info.counters.n_finished_tasks, Color::Green);
    row(&mut result, "CANCELED", info.counters.n_canceled_tasks, Color::Magenta);
    row(&mut result, "WAITING", info.counters.n_waiting_tasks(info.n_tasks), Color::Cyan);
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

pub fn print_job_detail(gsettings: &GlobalSettings, job: JobDetail, just_submitted: bool, show_tasks: bool) {
    let state_label = "State".cell().bold(true);
    let status = if just_submitted {
        vec![vec![state_label, "SUBMITTED".cell().foreground_color(Some(Color::Cyan))]]
    } else {
        if job.info.n_tasks == 1 {
            vec![vec![state_label, status_cell(job_status(&job.info))]]
        } else {
            let mut result = Vec::new();
            let mut it = job_status_with_counts_cells(&job.info).into_iter();
            result.push(vec![state_label, it.next().unwrap()]);
            result.extend(it.map(|c| vec!["".cell(), c]));
            result
        }
    };

    let mut rows = vec![
        vec!["Id".cell().bold(true), job.info.id.cell()],
        vec!["Name".cell().bold(true), job.info.name.cell()],
    ];

    rows.extend(status.into_iter());


    /*if let Some(error) = job.error {
        rows.push(vec![
            "Error".cell().bold(true),
            error.cell().foreground_color(Option::from(Color::Red)),
        ])
    }*/

    let mut n_tasks = job.info.n_tasks.to_string();
    if let JobType::Array(array_def) = job.job_type {
        n_tasks.push_str(&format!("; Ids: {}", array_def));
    }

    rows.push(vec![
        "Tasks".cell().bold(true),
        n_tasks.cell(),
    ]);

    // TODO: Each argument on own line, after the bug in cli-table is fixed
    let program_def = job.program_def;
    rows.push(vec![
        "Command".cell().bold(true),
        program_def.args.join("\n").cell(),
    ]);
    rows.push(vec![
        "Stdout".cell().bold(true),
        program_def
            .stdout
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "N/A".to_string())
            .cell(),
    ]);
    rows.push(vec![
        "Stdout".cell().bold(true),
        program_def
            .stderr
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "N/A".to_string())
            .cell(),
    ]);

    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());

    if !job.tasks.is_empty() {
        print_job_tasks(gsettings, job.tasks, show_tasks, &job.info.counters);
    }
}

fn print_job_tasks(gsettings: &GlobalSettings, mut tasks: Vec<JobTaskInfo>, show_tasks: bool, counters: &JobTaskCounters) {
    tasks.sort_unstable_by_key(|t| t.task_id);

    let make_error_row = |t: &JobTaskInfo| {
        match &t.state {
            JobTaskState::Failed(e) => { Some(vec![t.task_id.cell(), e.cell().foreground_color(Some(Color::Red))]) }
            _ => None
        }
    };

    if show_tasks {
        let rows : Vec<_> = tasks.iter().map(|t|
            vec![t.task_id.cell(),
                 status_cell(task_status(&t.state)),
                 match &t.state {
                     JobTaskState::Failed(e) => e.cell().foreground_color(Some(Color::Red)),
                     _ => "".cell()
                 }
            ]).collect();
        let table = rows.table().color_choice(gsettings.color_policy()).title(
            vec![
                "Task Id".cell().bold(true),
                "State".cell().bold(true),
                "Message".cell().bold(true),
            ]
        );
        assert!(print_stdout(table).is_ok());
    } else {
        const SHOWN_TASKS: usize = 5;
        let fail_rows: Vec<_> = tasks.iter().filter_map(make_error_row).take(SHOWN_TASKS).collect();

        if !fail_rows.is_empty() {
            let count = fail_rows.len() as JobTaskCount;
            let table = fail_rows.table().color_choice(gsettings.color_policy()).title(vec![
                "Task Id".cell().bold(true),
                "Error".cell().bold(true),
            ]);
            assert!(print_stdout(table).is_ok());

            if count < counters.n_failed_tasks {
                println!("{} tasks failed. ({} shown)", counters.n_failed_tasks, count);
            } else {
                println!("{} tasks failed.", counters.n_failed_tasks);
            }
        }
    }
}
