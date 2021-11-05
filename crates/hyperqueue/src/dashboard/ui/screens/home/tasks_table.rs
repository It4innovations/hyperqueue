use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row};

use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::progressbar::{ProgressPrintStyle, render_job_task_progress_bar};
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::server::job::JobTaskCounters;
use crate::transfer::messages::{JobDetail, JobType};

#[derive(Default)]
pub struct WorkerJobsTable {
    table: StatefulTable<WorkerJobRow>,
    state: Vec<(u32, JobDetail)>,
    current_worker: u32,
}

impl WorkerJobsTable {
    pub fn update(&mut self, job_detail: Vec<(u32, JobDetail)>) {
        self.state = job_detail;
    }

    pub fn update_current_worker(&mut self, worker_id: Option<u32>) {
        if let Some(worker_id) = worker_id {
            if self.current_worker != worker_id {
                self.current_worker = worker_id;
                let rows = create_rows(self.state.clone(), worker_id);
                self.table.set_items(rows);
                self.table.clear_selection();
            }
        }
    }

    pub fn select_next_job(&mut self) {
        self.table.select_next_wrap();
    }

    pub fn select_previous_job(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn get_selected_item(&self) -> Option<u32> {
        let selection = self.table.current_selection();
        selection.map(|row| row.id)
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame, selected_worker: Option<u32>) {
        if let Some(selected_worker) = selected_worker {
            self.table.draw(
                rect,
                frame,
                TableColumnHeaders {
                    title: format!("Jobs running on worker {}", selected_worker).to_string(),
                    inline_help: "".to_string(),
                    table_headers: Some(vec![
                        "id",
                        "program",
                        "job_type",
                        "status",
                        "max_fails",
                        "time_limit",
                    ]),
                    column_widths: vec![
                        Constraint::Percentage(5),
                        Constraint::Percentage(10),
                        Constraint::Percentage(10),
                        Constraint::Percentage(15),
                        Constraint::Percentage(15),
                        Constraint::Percentage(15),
                        Constraint::Percentage(15),
                        Constraint::Percentage(10),
                        Constraint::Percentage(10),
                    ],
                },
                |data| {
                    Row::new(vec![
                        Cell::from(data.id.to_string()),
                        Cell::from(data.info.to_string()),
                        Cell::from(data.job_type.to_string()),
                        Cell::from(render_job_task_progress_bar(&data.counters, ProgressPrintStyle::default())),
                        Cell::from(data.max_fails.to_string()),
                        Cell::from(data.time_limit.to_string()),
                    ])
                },
            );
        }
    }
}

struct WorkerJobRow {
    pub id: u32,
    pub info: String,
    pub job_type: String,
    pub counters: JobTaskCounters,
    pub max_fails: String,
    pub time_limit: String,
}

fn create_rows(detail: Vec<(u32, JobDetail)>, for_worker: u32) -> Vec<WorkerJobRow> {
    detail
        .iter()
        .filter_map(|(id, detail)| {
            if *id == for_worker {
                Some(WorkerJobRow {
                    id: detail.clone().info.id,
                    info: detail.clone().info.name,
                    job_type: create_job_type_string(detail.clone()),
                    counters: detail.clone().info.counters,
                    max_fails: create_max_fails_string(detail.clone()),
                    time_limit: create_time_limit_string(detail.clone()),
                })
            } else {
                None
            }
        })
        .collect()
}

fn create_task_ids_string(detail: JobDetail) -> String {
    let v = serde_json::to_value(&detail.tasks.iter().map(|x| x.task_id).collect::<Vec<u32>>())
        .unwrap();
    v.to_string()
}

fn create_job_type_string(detail: JobDetail) -> String {
    let v = serde_json::to_value(&detail.job_type).unwrap();
    let u: JobType = serde_json::from_value(v).unwrap();
    match u {
        JobType::Simple => "Simple".to_string(),
        JobType::Array(_) => "Array".to_string(),
    }
}

fn create_max_fails_string(detail: JobDetail) -> String {
    let v = serde_json::to_value(&detail.max_fails).unwrap();
    v.to_string()
}

fn create_priority_string(detail: JobDetail) -> String {
    let v = serde_json::to_value(&detail.priority).unwrap();
    v.to_string()
}

fn create_time_limit_string(detail: JobDetail) -> String {
    let v = serde_json::to_value(&detail.time_limit).unwrap();
    v.to_string()
}
