use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::WorkerId;
use std::time::SystemTime;
use tako::messages::common::WorkerConfiguration;
use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row};

#[derive(Default)]
pub struct WorkerInfoTable {
    table: StatefulTable<WorkerInfoDataRow>,
}

#[derive(Default, Debug)]
struct WorkerInfoDataRow {
    pub label: &'static str,
    pub data: String,
}

impl WorkerInfoTable {
    pub fn update(&mut self, data: &DashboardData, worker_id: Option<WorkerId>) {
        self.table.set_items(vec![]);
        if let Some(worker_id) = worker_id {
            if data
                .query_connected_worker_ids(SystemTime::now())
                .any(|id| id == worker_id)
            {
                let info = data.query_worker_info_for(&worker_id);
                if let Some(info) = info {
                    let rows = create_rows(info);
                    self.table.set_items(rows);
                }
            }
        }
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Worker Info".to_string(),
                inline_help: "".to_string(),
                table_headers: None,
                column_widths: vec![Constraint::Percentage(30), Constraint::Percentage(70)],
            },
            |data| Row::new(vec![Cell::from(data.label), Cell::from(data.data.as_str())]),
        );
    }
}

fn create_rows(worker_info: &WorkerConfiguration) -> Vec<WorkerInfoDataRow> {
    let missing_data_str = "_____".to_string();
    vec![
        WorkerInfoDataRow {
            label: "listen_address: ",
            data: worker_info.listen_address.to_string(),
        },
        WorkerInfoDataRow {
            label: "hostname: ",
            data: worker_info.hostname.to_string(),
        },
        WorkerInfoDataRow {
            label: "work_dir: ",
            data: (worker_info
                .work_dir
                .to_str()
                .unwrap_or_else(|| missing_data_str.as_str())
                .to_string()),
        },
        WorkerInfoDataRow {
            label: "log_dir: ",
            data: worker_info
                .log_dir
                .to_str()
                .unwrap_or_else(|| missing_data_str.as_str())
                .to_string(),
        },
        WorkerInfoDataRow {
            label: "heartbeat_interval: ",
            data: humantime::format_duration(worker_info.heartbeat_interval).to_string(),
        },
        WorkerInfoDataRow {
            label: "send_overview_interval: ",
            data: worker_info
                .send_overview_interval
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
        WorkerInfoDataRow {
            label: "idle_timeout: ",
            data: worker_info
                .idle_timeout
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
        WorkerInfoDataRow {
            label: "time_limit: ",
            data: worker_info
                .time_limit
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
    ]
}
