use crate::common::format::human_duration;
use crate::dashboard::ui::styles::table_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::transfer::messages::AllocationQueueParams;
use tako::worker::ServerLostPolicy;
use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row};

#[derive(Default)]
pub struct QueueParamsTable {
    table: StatefulTable<QueueParamsDataRow>,
}

#[derive(Default, Debug)]
struct QueueParamsDataRow {
    pub label: &'static str,
    pub data: String,
}

impl QueueParamsTable {
    pub fn update(&mut self, queue_params: &AllocationQueueParams) {
        let rows = create_rows(queue_params);
        self.table.set_items(rows);
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Allocation Queue Params",
                inline_help: "",
                table_headers: None,
                column_widths: vec![Constraint::Percentage(30), Constraint::Percentage(70)],
            },
            |data| Row::new(vec![Cell::from(data.label), Cell::from(data.data.as_str())]),
            table_style_deselected(),
        );
    }
}

fn create_rows(params: &AllocationQueueParams) -> Vec<QueueParamsDataRow> {
    vec![
        QueueParamsDataRow {
            label: "Workers Per Alloc: ",
            data: params.workers_per_alloc.to_string(),
        },
        QueueParamsDataRow {
            label: "Backlog: ",
            data: params.backlog.to_string(),
        },
        QueueParamsDataRow {
            label: "Time Limit: ",
            data: human_duration(chrono::Duration::from_std(params.timelimit).unwrap()),
        },
        QueueParamsDataRow {
            label: "Worker On Server Lost: ",
            data: match params.on_server_lost {
                ServerLostPolicy::Stop => "STOP".to_string(),
                ServerLostPolicy::FinishRunning => "FINISH_RUNNING".to_string(),
            },
        },
        QueueParamsDataRow {
            label: "Queue Name: ",
            data: params.name.clone().unwrap_or_default(),
        },
        QueueParamsDataRow {
            label: "Additional Args: ",
            data: params.additional_args.join(" "),
        },
        QueueParamsDataRow {
            label: "Worker CPU Arg: ",
            data: params.worker_cpu_arg.clone().unwrap_or_default(),
        },
        QueueParamsDataRow {
            label: "Worker Resource Args: ",
            data: params.worker_resources_args.join(" "),
        },
        QueueParamsDataRow {
            label: "Max Worker Count: ",
            data: params
                .max_worker_count
                .map(|count| count.to_string())
                .unwrap_or_default(),
        },
    ]
}
