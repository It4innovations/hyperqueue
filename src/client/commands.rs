use crate::messages::{JobInfo, JobState};
use cli_table::{format::Justify, print_stdout, Cell, Style, Table, Color};


pub fn print_job_stats(tasks: Vec<JobInfo>) {
    let rows : Vec<_> = tasks.into_iter().map(|t| {
        vec![t.id.cell().justify(Justify::Right),
             t.name.cell(),
             match t.state {
                 JobState::Waiting => { "WAITING".cell().foreground_color(Some(Color::Cyan)) }
                 JobState::Finished => { "FINISHED".cell().foreground_color(Some(Color::Green)) }
                 JobState::Failed => { "FAILED".cell().foreground_color(Some(Color::Red)) }
             },
             "TODO".cell(),
             "TODO".cell()]
    }).collect();

    let table = rows.table()
    .title(vec![
        "Id".cell().bold(true),
        "Name".cell().bold(true),
        "State".cell().bold(true),
        "Resources".cell().bold(true),
        "Walltime".cell().bold(true),
    ]);
    assert!(print_stdout(table).is_ok());
}
