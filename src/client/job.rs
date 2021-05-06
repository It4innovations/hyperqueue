use cli_table::format::Justify;
use cli_table::{print_stdout, Cell, CellStruct, Color, Style, Table};

use crate::client::globalsettings::GlobalSettings;
use crate::transfer::messages::{JobInfo, JobStatus};

fn job_state_cell(state: &JobStatus) -> CellStruct {
    match state {
        JobStatus::Waiting => "WAITING".cell().foreground_color(Some(Color::Cyan)),
        JobStatus::Finished => "FINISHED".cell().foreground_color(Some(Color::Green)),
        JobStatus::Failed => "FAILED".cell().foreground_color(Some(Color::Red)),
        JobStatus::Running => "RUNNING".cell().foreground_color(Some(Color::Yellow)),
        JobStatus::Submitted => "SUBMITTED".cell().foreground_color(Some(Color::Cyan)),
        JobStatus::Canceled => "CANCELED".cell().foreground_color(Some(Color::Magenta)),
    }
}

pub fn print_job_list(gsettings: &GlobalSettings, tasks: Vec<JobInfo>) {
    let rows: Vec<_> = tasks
        .into_iter()
        .map(|t| {
            vec![
                t.id.cell().justify(Justify::Right),
                t.name.cell(),
                job_state_cell(&t.status),
                // "TODO".cell(),
                // "TODO".cell(),
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
            // "Resources".cell().bold(true),
            // "Walltime".cell().bold(true),
        ]);
    assert!(print_stdout(table).is_ok());
}

pub fn print_job_detail(gsettings: &GlobalSettings, job: JobInfo) {
    let mut rows = vec![
        vec!["Id".cell().bold(true), job.id.cell()],
        vec!["Name".cell().bold(true), job.name.cell()],
        vec!["State".cell().bold(true), job_state_cell(&job.status)],
    ];

    if let Some(error) = job.error {
        rows.push(vec![
            "Error".cell().bold(true),
            error.cell().foreground_color(Option::from(Color::Red)),
        ])
    }

    // TODO: Each argument on own line, after the bug in cli-table is fixed
    if let Some(program_def) = job.spec {
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
    }

    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());
}
