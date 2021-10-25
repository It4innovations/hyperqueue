use tako::messages::gateway::CollectedOverview;
use tako::WorkerId;
use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row};

use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumns};
use crate::dashboard::utils::{calculate_memory_usage_percent, get_average_cpu_usage_for_worker};

#[derive(Default)]
pub struct WorkerUtilTable {
    table: StatefulTable<WorkerUtilRow>,
}

impl WorkerUtilTable {
    pub fn update(&mut self, overview: CollectedOverview) {
        let rows = create_rows(overview);
        self.table.set_items(rows);
    }

    pub fn select_next_worker(&mut self) {
        self.table.select_next_wrap();
    }

    pub fn select_previous_worker(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.table.draw(
            rect,
            frame,
            TableColumns {
                title: "worker hardware utilization".to_string(),
                inline_help: "".to_string(),
                table_headers: vec![
                    "worker_id",
                    "#tasks",
                    "cpu_util (%)",
                    "mem_util (%)",
                    "timestamp",
                ],
                column_widths: vec![
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                ],
            },
            |data| {
                Row::new(vec![
                    Cell::from(data.id.to_string()),
                    Cell::from(data.num_tasks.to_string()),
                    Cell::from(
                        data.average_cpu_usage
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "N/A".to_string()),
                    ),
                    Cell::from(
                        data.memory_usage
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "N/A".to_string()),
                    ),
                    Cell::from(
                        data.collection_timestamp
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "N/A".to_string()),
                    ),
                ])
            },
        );
    }
}

struct WorkerUtilRow {
    id: WorkerId,
    num_tasks: u32,
    average_cpu_usage: Option<f32>,
    memory_usage: Option<u64>,
    collection_timestamp: Option<u64>,
}

fn create_rows(overview: CollectedOverview) -> Vec<WorkerUtilRow> {
    overview
        .worker_overviews
        .iter()
        .map(|worker| {
            let hw_state = worker.hw_state.as_ref();
            let average_cpu_usage = hw_state.map(|s| get_average_cpu_usage_for_worker(s));
            let memory_usage =
                hw_state.map(|s| calculate_memory_usage_percent(&s.state.worker_memory_usage));
            let collection_timestamp = hw_state.map(|s| s.state.timestamp);

            WorkerUtilRow {
                id: worker.id,
                num_tasks: worker.running_tasks.len() as u32,
                average_cpu_usage,
                memory_usage,
                collection_timestamp,
            }
        })
        .collect()
}
