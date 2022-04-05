use crate::common::format::human_duration;
use crate::dashboard::data::job_timeline::{DashboardTaskState, TaskInfo};

use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::TakoTaskId;
use chrono::{DateTime, Local};
use std::time::SystemTime;
use tako::{TaskId, WorkerId};
use termion::event::Key;
use tui::layout::{Constraint, Rect};
use tui::style::{Color, Modifier, Style};
use tui::widgets::{Cell, Row};

// Task State Strings
const RUNNING: &str = "RUNNING";
const FINISHED: &str = "FINISHED";
const FAILED: &str = "FAILED";

#[derive(Default)]
pub struct TasksTable {
    table: StatefulTable<TaskRow>,
}

impl TasksTable {
    pub fn update(&mut self, tasks_info: Vec<(&TakoTaskId, &TaskInfo)>) {
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

    pub fn get_selected_item(&self) -> Option<(TaskId, WorkerId)> {
        let selection = self.table.current_selection();
        selection.map(|row| (row.task_id, row.worker_id))
    }

    pub fn handle_key(&mut self, key: Key) {
        match key {
            Key::Down => self.select_next_task(),
            Key::Up => self.select_previous_task(),
            _ => {}
        }
    }

    pub fn draw(
        &mut self,
        title: &'static str,
        rect: Rect,
        frame: &mut DashboardFrame,
        table_style: Style,
    ) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title,
                inline_help: "",
                table_headers: Some(vec!["Task ID", "State", "Start", "End", "Makespan"]),
                column_widths: vec![
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                ],
            },
            |task_row| {
                Row::new(vec![
                    Cell::from(task_row.task_id.to_string()),
                    Cell::from(task_row.task_state.as_str())
                        .style(get_task_state_color(&task_row.task_state)),
                    Cell::from(task_row.start_time.as_str()),
                    Cell::from(task_row.end_time.as_str()),
                    Cell::from(task_row.run_time.as_str()),
                ])
            },
            table_style,
        );
    }
}

struct TaskRow {
    worker_id: WorkerId,
    task_id: TaskId,
    task_state: String,
    start_time: String,
    end_time: String,
    run_time: String,
}

fn create_rows(mut rows: Vec<(&TakoTaskId, &TaskInfo)>) -> Vec<TaskRow> {
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
                    end_time.format("%b %e, %T").to_string()
                })
                .unwrap_or_else(|| "".to_string());

            let run_time = match task_info.end_time {
                None => SystemTime::now().duration_since(task_info.start_time),
                Some(end_time) => end_time.duration_since(task_info.start_time),
            }
            .unwrap_or_default();

            TaskRow {
                task_id: **task_id,
                worker_id: task_info.worker_id,
                task_state: match task_info.get_task_state_at(SystemTime::now()).unwrap() {
                    DashboardTaskState::Running => RUNNING.to_string(),
                    DashboardTaskState::Finished => FINISHED.to_string(),
                    DashboardTaskState::Failed => FAILED.to_string(),
                },
                run_time: human_duration(chrono::Duration::from_std(run_time).unwrap()),
                start_time: start_time.format("%b %e, %T").to_string(),
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
