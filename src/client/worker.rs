use cli_table::format::Justify;
use cli_table::{print_stdout, Cell, Color, Style, Table};

use crate::client::globalsettings::GlobalSettings;
use crate::transfer::messages::WorkerInfo;

pub fn print_worker_info(workers: Vec<WorkerInfo>, gsettings: &GlobalSettings) {
    let rows: Vec<_> = workers
        .into_iter()
        .map(|w| {
            vec![
                w.id.cell().justify(Justify::Right),
                match w.ended_at {
                    None => "RUNNING".cell().foreground_color(Some(Color::Green)),
                    Some(_) => "OFFLINE".cell().foreground_color(Some(Color::Red)),
                },
                w.configuration.hostname.cell(),
                w.configuration.resources.summary().cell(),
                w.configuration
                    .extra
                    .get("MANAGER")
                    .map(|x| x.as_str())
                    .unwrap_or("None")
                    .cell(),
                w.configuration
                    .extra
                    .get("MANAGER_JOB_ID")
                    .map(|x| x.as_str())
                    .unwrap_or("N/A")
                    .cell(),
            ]
        })
        .collect();

    let table = rows
        .table()
        .color_choice(gsettings.color_policy())
        .title(vec![
            "Id".cell(),
            "State".cell().bold(true),
            "Hostname".cell().bold(true),
            "Resources".cell().bold(true),
            "Manager".cell().bold(true),
            "Manager Job Id".cell().bold(true),
        ]);
    assert!(print_stdout(table).is_ok());
}
