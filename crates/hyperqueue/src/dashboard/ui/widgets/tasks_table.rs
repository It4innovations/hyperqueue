use crate::common::format::human_duration;
use crate::dashboard::data::timelines::job_timeline::{DashboardTaskState, TaskInfo};

use crate::JobTaskId;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use chrono::{DateTime, Local};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Cell, Row};
use std::time::SystemTime;
use tako::WorkerId;

// Task State Strings
const RUNNING: &str = "RUNNING";
const FINISHED: &str = "FINISHED";
const FAILED: &str = "FAILED";

pub struct TasksTable {
    table: StatefulTable<TaskRow>,
    interactive: bool,
}

impl TasksTable {
    pub fn interactive() -> Self {
        Self {
            table: Default::default(),
            interactive: true,
        }
    }
    pub fn non_interactive() -> Self {
        Self {
            table: Default::default(),
            interactive: false,
        }
    }

    pub fn update(&mut self, tasks_info: Vec<(JobTaskId, &TaskInfo)>) {
        let rows = create_rows(tasks_info);
        self.table.set_items(rows);
    }

    pub fn select_next_task(&mut self) {
        self.table.select_next_wrap();
    }

    pub fn clear_selection(&mut self) {
        self.table.clear_selection();
    }

    pub fn select_previous_task(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn get_selected_item(&self) -> Option<(JobTaskId, WorkerId)> {
        let selection = self.table.current_selection();
        selection.map(|row| (row.task_id, row.worker_id))
    }

    pub fn handle_key(&mut self, key: KeyEvent) {
        if self.interactive {
            match key.code {
                KeyCode::Down => self.select_next_task(),
                KeyCode::Up => self.select_previous_task(),
                _ => {}
            }
        }
    }

    pub fn draw(
        &mut self,
        title: &'static str,
        rect: Rect,
        frame: &mut DashboardFrame,
        with_worker: bool,
        table_style: Style,
    ) {
        let mut headers = vec!["Task ID"];
        if with_worker {
            headers.push("Worker ID");
        }
        headers.extend(["State", "Start", "End", "Makespan"]);
        let mut column_widths = vec![Constraint::Max(8), Constraint::Max(10)];
        column_widths.extend(std::iter::repeat(Constraint::Fill(1)).take(headers.len() - 2));

        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title,
                table_headers: Some(headers),
                column_widths,
            },
            |task_row| {
                let mut cols = vec![Cell::from(task_row.task_id.to_string())];
                if with_worker {
                    cols.push(Cell::from(task_row.worker_id.to_string()));
                }
                cols.extend([
                    Cell::from(task_row.task_state.as_str())
                        .style(get_task_state_color(&task_row.task_state)),
                    Cell::from(task_row.start_time.as_str()),
                    Cell::from(task_row.end_time.as_str()),
                    Cell::from(task_row.run_time.as_str()),
                ]);
                Row::new(cols)
            },
            table_style,
        );
    }
}

struct TaskRow {
    worker_id: WorkerId,
    task_id: JobTaskId,
    task_state: String,
    start_time: String,
    end_time: String,
    run_time: String,
}

fn create_rows(mut rows: Vec<(JobTaskId, &TaskInfo)>) -> Vec<TaskRow> {
    rows.sort_by_key(|(_, task_info)| {
        let status_index = match task_info.get_task_state_at(SystemTime::now()).unwrap() {
            DashboardTaskState::Running => 0,
            DashboardTaskState::Finished => 1,
            DashboardTaskState::Failed => 2,
        };
        match task_info.end_time {
            None => (status_index, task_info.start_time),
            Some(end_time) => (status_index, end_time),
        }
    });

    rows.iter()
        .map(|(task_id, task_info)| {
            let start_time: DateTime<Local> = task_info.start_time.into();

            let end_time = task_info
                .end_time
                .map(|time| {
                    let end_time: DateTime<Local> = time.into();
                    end_time.format("%d.%m. %H:%M:%S").to_string()
                })
                .unwrap_or_else(|| "".to_string());

            let run_time = match task_info.end_time {
                None => SystemTime::now().duration_since(task_info.start_time),
                Some(end_time) => end_time.duration_since(task_info.start_time),
            }
            .unwrap_or_default();

            TaskRow {
                task_id: *task_id,
                worker_id: task_info.worker_id,
                task_state: match task_info.get_task_state_at(SystemTime::now()).unwrap() {
                    DashboardTaskState::Running => RUNNING.to_string(),
                    DashboardTaskState::Finished => FINISHED.to_string(),
                    DashboardTaskState::Failed => FAILED.to_string(),
                },
                run_time: human_duration(chrono::Duration::from_std(run_time).unwrap()),
                start_time: start_time.format("%d.%m. %H:%M:%S").to_string(),
                end_time,
            }
        })
        .collect()
}

pub fn get_task_state_color(task_status: &str) -> Style {
    let color = if task_status == RUNNING {
        Color::Yellow
    } else if task_status == FINISHED {
        Color::Green
    } else {
        Color::Red
    };

    Style {
        fg: Some(color),
        bg: None,
        add_modifier: Modifier::empty(),
        sub_modifier: Modifier::empty(),
    }
}
