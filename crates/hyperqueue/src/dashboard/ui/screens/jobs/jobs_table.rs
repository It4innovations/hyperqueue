use crate::dashboard::data::DashboardData;
use crate::dashboard::data::timelines::job_timeline::DashboardJobInfo;

use crate::dashboard::ui::terminal::DashboardFrame;

use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};

use crate::JobId;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Alignment, Constraint, Rect};
use ratatui::style::Style;
use ratatui::text::Text;
use ratatui::widgets::{Cell, Row};

#[derive(Default)]
pub struct JobsTable {
    table: StatefulTable<JobInfoRow>,
}

impl JobsTable {
    pub fn update(&mut self, data: &DashboardData) {
        let jobs: Vec<(JobId, &DashboardJobInfo)> = data
            .query_jobs_created_before(data.current_time())
            .collect();
        let rows = create_rows(jobs);
        self.table.set_items(rows);
    }

    fn select_next_job(&mut self) {
        self.table.select_next_wrap();
    }

    fn select_previous_job(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn get_selected_item(&self) -> Option<JobId> {
        let selection = self.table.current_selection();
        selection.map(|row| row.id)
    }

    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => self.select_next_job(),
            KeyCode::Up => self.select_previous_job(),
            _ => {}
        }
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame, table_style: Style) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Jobs <1>",
                table_headers: Some(vec!["Job ID", "Name"]),
                column_widths: vec![Constraint::Percentage(20), Constraint::Percentage(80)],
            },
            |data| {
                Row::new(vec![
                    Cell::from(Text::from(data.id.to_string()).alignment(Alignment::Right)),
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

fn create_rows(job_infos: Vec<(JobId, &DashboardJobInfo)>) -> Vec<JobInfoRow> {
    job_infos
        .iter()
        .map(|(job_id, info)| JobInfoRow {
            id: *job_id,
            name: info.job.name.clone(),
        })
        .collect()
}
