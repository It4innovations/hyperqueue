use cli_table::format::Justify;
use cli_table::{print_stdout, Cell, CellStruct, Color, Style, Table};

use crate::client::globalsettings::GlobalSettings;
use crate::common::manager::info::GetManagerInfo;
use crate::transfer::messages::{LostWorkerReasonInfo, WorkerExitInfo, WorkerInfo};

pub enum WorkerState {
    Running,
    ConnectionLost,
    HeartbeatLost,
}

fn worker_state(worker: &WorkerInfo) -> CellStruct {
    match worker.ended {
        None => "RUNNING".cell().foreground_color(Some(Color::Green)),
        Some(WorkerExitInfo {
            reason: LostWorkerReasonInfo::ConnectionLost,
            ..
        }) => "CONNECTION LOST".cell().foreground_color(Some(Color::Red)),
        Some(WorkerExitInfo {
            reason: LostWorkerReasonInfo::HeartbeatLost,
            ..
        }) => "HEARTBEAT LOST".cell().foreground_color(Some(Color::Red)),
        Some(WorkerExitInfo {
            reason: LostWorkerReasonInfo::IdleTimeout,
            ..
        }) => "IDLE TIMEOUT".cell().foreground_color(Some(Color::Cyan)),
        Some(WorkerExitInfo {
            reason: LostWorkerReasonInfo::Stopped,
            ..
        }) => "STOPPED".cell().foreground_color(Some(Color::Magenta)),
    }
}

pub fn print_worker_info(workers: Vec<WorkerInfo>, gsettings: &GlobalSettings) {
    let rows: Vec<_> = workers
        .into_iter()
        .map(|w| {
            let manager_info = w.configuration.get_manager_info();
            vec![
                w.id.cell().justify(Justify::Right),
                worker_state(&w),
                w.configuration.hostname.cell(),
                w.configuration.resources.summary().cell(),
                manager_info
                    .as_ref()
                    .map(|info| info.manager.to_string())
                    .unwrap_or_else(|| "None".to_string())
                    .cell(),
                manager_info
                    .as_ref()
                    .map(|info| info.job_id.as_str())
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
