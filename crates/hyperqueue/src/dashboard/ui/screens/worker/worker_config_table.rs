use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use tako::messages::common::WorkerConfiguration;
use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row};

#[derive(Default)]
pub struct WorkerConfigTable {
    table: StatefulTable<WorkerConfigDataRow>,
}

#[derive(Default, Debug)]
struct WorkerConfigDataRow {
    pub label: &'static str,
    pub data: String,
}

impl WorkerConfigTable {
    pub fn update(&mut self, configuration: &WorkerConfiguration) {
        let rows = create_rows(configuration);
        self.table.set_items(rows);
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Worker Configuration",
                inline_help: "",
                table_headers: None,
                column_widths: vec![Constraint::Percentage(30), Constraint::Percentage(70)],
            },
            |data| Row::new(vec![Cell::from(data.label), Cell::from(data.data.as_str())]),
        );
    }
}

fn create_rows(worker_info: &WorkerConfiguration) -> Vec<WorkerConfigDataRow> {
    let missing_data_str = String::new();
    vec![
        WorkerConfigDataRow {
            label: "Listen Address: ",
            data: worker_info.listen_address.to_string(),
        },
        WorkerConfigDataRow {
            label: "Hostname: ",
            data: worker_info.hostname.to_string(),
        },
        WorkerConfigDataRow {
            label: "Work Dir: ",
            data: (worker_info
                .work_dir
                .to_str()
                .unwrap_or_else(|| missing_data_str.as_str())
                .to_string()),
        },
        WorkerConfigDataRow {
            label: "Log Dir: ",
            data: worker_info
                .log_dir
                .to_str()
                .unwrap_or_else(|| missing_data_str.as_str())
                .to_string(),
        },
        WorkerConfigDataRow {
            label: "Heartbeat Interval: ",
            data: humantime::format_duration(worker_info.heartbeat_interval).to_string(),
        },
        WorkerConfigDataRow {
            label: "Send Overview Interval: ",
            data: worker_info
                .send_overview_interval
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
        WorkerConfigDataRow {
            label: "Idle Timeout: ",
            data: worker_info
                .idle_timeout
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
        WorkerConfigDataRow {
            label: "Time Limit: ",
            data: worker_info
                .time_limit
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
    ]
}
