use std::io::Stdout;

use tako::messages::gateway::CollectedOverview;
use tako::WorkerId;
use termion::input::MouseTerminal;
use termion::raw::RawTerminal;
use termion::screen::AlternateScreen;
use tui::backend::TermionBackend;
use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row};
use tui::Frame;

use crate::dashboard::models::StatefulTable;
use crate::dashboard::ui::screens::draw_utils::{draw_table, ResourceTableProps};
use crate::dashboard::utils::calculate_memory_usage_percent;

struct WorkerUtilTableCols {
    id: WorkerId,
    num_tasks: i32,
    average_cpu_usage: f32,
    memory_usage: u64,
    collection_timestamp: u64,
}

/// Draws the worker hardware utilization dashboard table with the data
pub fn draw_worker_utilization_table(
    in_chunk: Rect,
    frame: &mut Frame<TermionBackend<AlternateScreen<MouseTerminal<RawTerminal<Stdout>>>>>,
    data: CollectedOverview,
) {
    let mut stateful_table: StatefulTable<WorkerUtilTableCols> = StatefulTable::new();

    let table_columns = WorkerUtilTableCols::from(data);
    stateful_table.set_items(table_columns);

    draw_table(
        frame,
        in_chunk,
        ResourceTableProps {
            title: "worker hardware utilization".to_string(),
            inline_help: "".to_string(),
            resource: &mut stateful_table,
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
    );
}

/// The columns in the Worker Utilization Table from Overview
impl WorkerUtilTableCols {
    fn from(overview: CollectedOverview) -> Vec<Self> {
        let mut util_vec: Vec<WorkerUtilTableCols> = vec![];

        for overview in overview.worker_overviews {
            if let Some(hw_overview) = overview.hw_state {
                let num_cpus = hw_overview
                    .state
                    .worker_cpu_usage
                    .cpu_per_core_percent_usage
                    .len();
                let cpu_usage_sum_per_core = hw_overview
                    .state
                    .worker_cpu_usage
                    .cpu_per_core_percent_usage
                    .into_iter()
                    .reduce(|cpu_a, cpu_b| (cpu_a + cpu_b))
                    .unwrap();
                util_vec.push(WorkerUtilTableCols {
                    id: overview.id,
                    num_tasks: overview.running_tasks.len() as i32,
                    average_cpu_usage: cpu_usage_sum_per_core / num_cpus as f32,
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

impl Default for WorkerUtilTableCols {
    fn default() -> Self {
        Self {
            id: 0,
            num_tasks: 0,
            average_cpu_usage: 0.0,
            memory_usage: 0,
            collection_timestamp: 0,
        }
    }
}
