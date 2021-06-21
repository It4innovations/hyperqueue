use cli_table::{print_stdout, Cell, Style, Table};
use humantime::format_duration;
use tako::messages::common::WorkerConfiguration;

use crate::client::globalsettings::GlobalSettings;
use crate::WorkerId;

pub fn print_worker_configuration(
    gsettings: &GlobalSettings,
    worker_id: WorkerId,
    configuration: WorkerConfiguration,
) {
    let rows = vec![
        vec!["Worker ID".cell().bold(true), worker_id.cell()],
        vec!["Hostname".cell().bold(true), configuration.hostname.cell()],
        vec![
            "Data provider".cell().bold(true),
            configuration.listen_address.cell(),
        ],
        vec![
            "Working directory".cell().bold(true),
            configuration.work_dir.display().cell(),
        ],
        vec![
            "Logging directory".cell().bold(true),
            configuration.log_dir.display().cell(),
        ],
        vec![
            "Heartbeat".cell().bold(true),
            format_duration(configuration.heartbeat_interval)
                .to_string()
                .cell(),
        ],
        vec![
            "Idle timeout".cell().bold(true),
            configuration
                .idle_timeout
                .map(|x| format_duration(x).to_string())
                .unwrap_or("None".to_string())
                .cell(),
        ],
        vec![
            "Resources".cell().bold(true),
            configuration.resources.summary().cell(),
        ],
        vec![
            "Manager".cell().bold(true),
            configuration
                .extra
                .get("MANAGER")
                .map(|x| x.as_str())
                .unwrap_or("None")
                .cell(),
        ],
        vec![
            "Manager Job Id".cell().bold(true),
            configuration
                .extra
                .get("MANAGER_JOB_ID")
                .map(|x| x.as_str())
                .unwrap_or("N/A")
                .cell(),
        ],
    ];
    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());
}
