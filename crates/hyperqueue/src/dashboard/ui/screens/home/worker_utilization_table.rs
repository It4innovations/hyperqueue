use tako::messages::gateway::CollectedOverview;
use tako::WorkerId;
use tui::layout::Rect;

use crate::dashboard::ui::screens::draw_utils::StatefulTable;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::utils::{calculate_memory_usage_percent, get_average_cpu_usage_for_worker};

pub struct HwUtilTable {
    stateful_table: StatefulTable<WorkerUtilTableCols>,
}

struct WorkerUtilTableCols {
    id: WorkerId,
    num_tasks: i32,
    average_cpu_usage: f32,
    memory_usage: u64,
    collection_timestamp: u64,
}

impl HwUtilTable {
    pub fn new() -> Self {
        let stateful_table: StatefulTable<WorkerUtilTableCols> = StatefulTable::new();
        Self { stateful_table }
    }

    fn draw(&self, rect: Rect, frame: &mut DashboardFrame, data: Option<CollectedOverview>) {
        //fixme: don't unwrap
        /*let table_columns = WorkerUtilTableCols::from(data.unwrap());
        self.stateful_table.set_items(table_columns);

        draw_table(
            frame,
            rect,
            ResourceTableProps {
                title: "worker hardware utilization".to_string(),
                inline_help: "".to_string(),
                resource: &mut self.stateful_table.clone(),
                table_headers: vec![
                    "worker_id",
                    "#tasks",
                    "cpu_util (%)",
                    "mem_util(%)",
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
                    Cell::from(data.average_cpu_usage.to_string()),
                    Cell::from(data.memory_usage.to_string()),
                    Cell::from(data.collection_timestamp.to_string()),
                ])
            },
            true,
        );*/
    }
}

/// The columns in the Worker Utilization Table from Overview
impl WorkerUtilTableCols {
    fn from(overview: CollectedOverview) -> Vec<Self> {
        let mut util_vec: Vec<WorkerUtilTableCols> = vec![];

        for overview in overview.worker_overviews {
            if let Some(hw_overview) = overview.hw_state {
                let avg_cpu_usage = get_average_cpu_usage_for_worker(hw_overview.clone());

                util_vec.push(WorkerUtilTableCols {
                    id: overview.id,
                    num_tasks: overview.running_tasks.len() as i32,
                    average_cpu_usage: avg_cpu_usage,
                    memory_usage: calculate_memory_usage_percent(
                        hw_overview.state.worker_memory_usage,
                    ),
                    collection_timestamp: hw_overview.state.timestamp,
                });
            }
        }
        util_vec
    }
}
