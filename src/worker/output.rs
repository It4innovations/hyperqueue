use tako::messages::common::WorkerConfiguration;
use crate::WorkerId;
use cli_table::{Cell, Style, Table, print_stdout};
use humantime::format_duration;
use crate::client::globalsettings::GlobalSettings;

pub fn print_worker_configuration(gsettings: &GlobalSettings, worker_id: WorkerId, configuration: WorkerConfiguration) {
    let rows = vec![
        vec!["Worker ID".cell().bold(true), worker_id.cell()],
        vec!["Hostname".cell().bold(true), configuration.hostname.cell()],
        vec!["Data provider".cell().bold(true), configuration.listen_address.cell()],
        vec!["Working directory".cell().bold(true), configuration.work_dir.display().cell()],
        vec!["Logging directory".cell().bold(true), configuration.log_dir.display().cell()],
        vec!["Heartbeat".cell().bold(true), format_duration(configuration.heartbeat_interval).to_string().cell()],
        vec!["# of cpus".cell().bold(true), configuration.n_cpus.cell()],
    ];
    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());
}