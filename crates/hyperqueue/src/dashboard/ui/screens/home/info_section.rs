use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::transfer::messages::WorkerInfo;
use crate::WorkerId;
use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row};

#[derive(Default)]
pub struct WorkerInfoTable {
    table: StatefulTable<WorkerInfoDataRow>,
    data: WorkerInfoData,
}

#[derive(Default)]
struct WorkerInfoData {
    worker_info: Vec<WorkerInfo>,
}

#[derive(Default, Debug)]
struct WorkerInfoDataRow {
    pub label: String,
    pub data: String,
}

impl WorkerInfoData {
    pub fn get_worker_info_for(&self, current_worker_id: WorkerId) -> Option<&WorkerInfo> {
        self.worker_info
            .iter()
            .find(|info| info.id == current_worker_id)
    }
}

impl WorkerInfoTable {
    pub fn update(&mut self, worker_info: Vec<WorkerInfo>) {
        self.data = WorkerInfoData { worker_info };
    }

    pub fn change_info(&mut self, selected_worker: Option<WorkerId>) {
        if let Some(id) = selected_worker {
            if let Some(info) = self.data.get_worker_info_for(id) {
                let rows = create_rows(info);
                self.table.set_items(rows);
            }
        } else {
            self.table.set_items(vec![]);
        }
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame, worker_id: Option<u32>) {
        if let Some(worker_id) = worker_id {
            self.table.draw(
                rect,
                frame,
                TableColumnHeaders {
                    title: format!("Worker Info for worker_id = ({})", worker_id).to_string(),
                    inline_help: "".to_string(),
                    table_headers: None,
                    column_widths: vec![Constraint::Percentage(30), Constraint::Percentage(70)],
                },
                |data| {
                    Row::new(vec![
                        Cell::from(data.label.to_string()),
                        Cell::from(data.data.to_string()),
                    ])
                },
            );
        }
    }
}

fn create_rows(worker_info: &WorkerInfo) -> Vec<WorkerInfoDataRow> {
    let missing_data_str = "_____".to_string();
    vec![
        WorkerInfoDataRow {
            label: "listen_address: ".to_string(),
            data: worker_info.configuration.listen_address.to_string(),
        },
        WorkerInfoDataRow {
            label: "hostname: ".to_string(),
            data: worker_info.configuration.hostname.to_string(),
        },
        WorkerInfoDataRow {
            label: "work_dir: ".to_string(),
            data: (worker_info
                .configuration
                .work_dir
                .to_str()
                .unwrap_or_else(|| missing_data_str.as_str())
                .to_string()),
        },
        WorkerInfoDataRow {
            label: "log_dir: ".to_string(),
            data: worker_info
                .configuration
                .log_dir
                .to_str()
                .unwrap_or_else(|| missing_data_str.as_str())
                .to_string(),
        },
        WorkerInfoDataRow {
            label: "heartbeat_interval: ".to_string(),
            data: humantime::format_duration(worker_info.configuration.heartbeat_interval)
                .to_string(),
        },
        WorkerInfoDataRow {
            label: "hw_state_poll_interval: ".to_string(),
            data: worker_info
                .configuration
                .hw_state_poll_interval
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
        WorkerInfoDataRow {
            label: "idle_timeout: ".to_string(),
            data: worker_info
                .configuration
                .idle_timeout
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
        WorkerInfoDataRow {
            label: "time_limit: ".to_string(),
            data: worker_info
                .configuration
                .time_limit
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
    ]
}
