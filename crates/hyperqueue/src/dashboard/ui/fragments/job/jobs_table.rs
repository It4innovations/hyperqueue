use crate::dashboard::data::timelines::job_timeline::DashboardJobInfo;
use crate::dashboard::data::DashboardData;

use crate::dashboard::ui::terminal::DashboardFrame;

use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};

use crate::JobId;
use std::time::SystemTime;

use ratatui::layout::{Constraint, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Cell, Row};
use termion::event::Key;

#[derive(Default)]
pub struct JobsTable {
    table: StatefulTable<JobInfoRow>,
}

impl JobsTable {
    pub fn update(&mut self, data: &DashboardData) {
        let jobs: Vec<(&JobId, &DashboardJobInfo)> =
            data.query_jobs_created_before(SystemTime::now()).collect();
        let rows = create_rows(jobs);
        self.table.set_items(rows);
    }

    pub fn select_next_job(&mut self) {
        self.table.select_next_wrap();
    }

    pub fn select_previous_job(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn get_selected_item(&self) -> Option<JobId> {
        let selection = self.table.current_selection();
        selection.map(|row| row.id)
    }

    pub fn handle_key(&mut self, key: Key) {
        match key {
            Key::Down => self.select_next_job(),
            Key::Up => self.select_previous_job(),
            _ => {}
        }
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame, table_style: Style) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Jobs <1>",
                inline_help: "",
                table_headers: Some(vec!["Job ID", "Name"]),
                column_widths: vec![Constraint::Percentage(20), Constraint::Percentage(80)],
            },
            |data| {
                Row::new(vec![
                    Cell::from(data.id.to_string()),
                    Cell::from(data.name.as_str()),
                ])
            },
            table_style,
        );
    }
}

struct JobInfoRow {
    id: JobId,
    name: String,
}

fn create_rows(job_infos: Vec<(&JobId, &DashboardJobInfo)>) -> Vec<JobInfoRow> {
    job_infos
        .iter()
        .map(|(job_id, info)| JobInfoRow {
            id: **job_id,
            name: info.job_info.name.clone(),
        })
        .collect()
}
