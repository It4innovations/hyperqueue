use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::progressbar::{
    get_progress_bar_color, render_progress_bar_at, ProgressPrintStyle,
};
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use tako::WorkerId;
use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row};

#[derive(Default)]
pub struct WorkerUtilTable {
    table: StatefulTable<WorkerUtilRow>,
}

impl WorkerUtilTable {
    pub fn update(&mut self, _data: &DashboardData) {
        // if let Some(overview) = data.get_latest_overview() {
        //     let rows = create_rows(overview);
        //     self.table.set_items(rows);
        // }
        // TODO
    }

    pub fn select_next_worker(&mut self) {
        self.table.select_next_wrap();
    }

    pub fn select_previous_worker(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn get_selected_item(&self) -> Option<WorkerId> {
        let selection = self.table.current_selection();
        selection.map(|row| row.id)
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "worker hardware utilization".to_string(),
                inline_help: "".to_string(),
                table_headers: Some(vec![
                    "worker_id",
                    "#running tasks",
                    "cpu_util (%)",
                    "mem_util (%)",
                ]),
                column_widths: vec![
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                ],
            },
            |data| {
                let cpu_progress = (data.average_cpu_usage.unwrap_or(0.0)) / 100.0;
                let mem_progress = (data.memory_usage.unwrap_or(0) as f32) / 100.0;
                let cpu_prog_bar =
                    render_progress_bar_at(cpu_progress, 12, ProgressPrintStyle::default());

                let mem_prog_bar =
                    render_progress_bar_at(mem_progress, 12, ProgressPrintStyle::default());

                Row::new(vec![
                    Cell::from(data.id.to_string()),
                    Cell::from(data.num_tasks.to_string()),
                    Cell::from(cpu_prog_bar).style(get_progress_bar_color(cpu_progress)),
                    Cell::from(mem_prog_bar).style(get_progress_bar_color(mem_progress)),
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
}

// TODO
/*fn create_rows(overview: &CollectedOverview) -> Vec<WorkerUtilRow> {
    overview
        .worker_overviews
        .iter()
        .map(|worker| {
            let hw_state = worker.hw_state.as_ref();
            let average_cpu_usage = hw_state.map(get_average_cpu_usage_for_worker);
            let memory_usage =
                hw_state.map(|s| calculate_memory_usage_percent(&s.state.worker_memory_usage));

            WorkerUtilRow {
                id: worker.id,
                num_tasks: worker.running_tasks.len() as u32,
                average_cpu_usage,
                memory_usage,
            }
        })
        .collect()
}*/