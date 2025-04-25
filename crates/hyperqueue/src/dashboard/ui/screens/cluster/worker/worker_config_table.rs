use crate::dashboard::ui::styles::table_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Cell, Row};
use tako::worker::WorkerConfiguration;

#[derive(Default)]
pub struct WorkerConfigTable {
    table: StatefulTable<WorkerConfigDataRow>,
}

#[derive(Default, Debug)]
struct WorkerConfigDataRow {
    label: &'static str,
    data: String,
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
                table_headers: None,
                column_widths: vec![Constraint::Percentage(30), Constraint::Percentage(70)],
            },
            |data| Row::new(vec![Cell::from(data.label), Cell::from(data.data.as_str())]),
            table_style_deselected(),
        );
    }
}

fn create_rows(worker_info: &WorkerConfiguration) -> Vec<WorkerConfigDataRow> {
    let missing_data_str = String::new();
    vec![
        WorkerConfigDataRow {
            label: "Listen Address:",
            data: worker_info.listen_address.to_string(),
        },
        WorkerConfigDataRow {
            label: "Hostname:",
            data: worker_info.hostname.to_string(),
        },
        WorkerConfigDataRow {
            label: "Workdir:",
            data: worker_info
                .work_dir
                .to_str()
                .unwrap_or(missing_data_str.as_str())
                .to_string(),
        },
        WorkerConfigDataRow {
            label: "Heartbeat Interval:",
            data: humantime::format_duration(worker_info.heartbeat_interval).to_string(),
        },
        WorkerConfigDataRow {
            label: "Send Overview Interval:",
            data: worker_info
                .overview_configuration
                .send_interval
                .as_ref()
                .map(|send_interval| humantime::format_duration(*send_interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
        WorkerConfigDataRow {
            label: "Idle Timeout:",
            data: worker_info
                .idle_timeout
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
        WorkerConfigDataRow {
            label: "Time Limit:",
            data: worker_info
                .time_limit
                .map(|interval| humantime::format_duration(interval).to_string())
                .unwrap_or_else(|| missing_data_str.clone()),
        },
    ]
}
