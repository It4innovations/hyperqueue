use crate::common::format::human_duration;
use crate::dashboard::ui::styles::table_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::transfer::messages::AllocationQueueParams;
use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Cell, Row};

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
    pub fn update(&mut self, queue_params: Option<&AllocationQueueParams>) {
        let rows = match queue_params {
            Some(params) => create_rows(params),
            None => vec![],
        };
        self.table.set_items(rows);
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Allocation Queue Params",
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
            label: "Maximum Workers Per Alloc: ",
            data: params.max_workers_per_alloc.to_string(),
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
            label: "Worker Args: ",
            data: params.worker_args.join(" "),
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
            label: "Max Worker Count: ",
            data: params
                .max_worker_count
                .map(|count| count.to_string())
                .unwrap_or_default(),
        },
    ]
}
