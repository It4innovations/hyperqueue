use crate::dashboard::data::timelines::job_timeline::DashboardJobInfo;
use crate::dashboard::ui::styles::table_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::transfer::messages::JobTaskDescription;
use chrono::{DateTime, Local};
use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Cell, Row};
use std::borrow::Cow;

#[derive(Default)]
pub struct JobInfoTable {
    table: StatefulTable<JobInfoDataRow>,
}

#[derive(Default, Debug)]
struct JobInfoDataRow {
    pub label: &'static str,
    pub data: Cow<'static, str>,
}

impl JobInfoTable {
    pub fn update(&mut self, info: &DashboardJobInfo) {
        let rows = create_rows(info);
        self.table.set_items(rows);
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Job Details",
                table_headers: None,
                column_widths: vec![Constraint::Percentage(30), Constraint::Percentage(70)],
            },
            |data| Row::new(vec![Cell::from(data.label), Cell::from(data.data.as_ref())]),
            table_style_deselected(),
        );
    }
}
fn create_rows(params: &DashboardJobInfo) -> Vec<JobInfoDataRow> {
    let creation_time: DateTime<Local> = params.job_creation_time.into();
    let completion_time: Option<Cow<'static, str>> = params.completion_date.map(|time| {
        let end_time: DateTime<Local> = time.into();
        end_time.format("%b %e, %T").to_string().into()
    });
    // let log_path = params
    //     .job_info
    //     .job_desc
    //     .log
    //     .as_ref()
    //     .map(|log_path| log_path.to_string_lossy().to_string())
    //     .unwrap_or_default();

    vec![
        JobInfoDataRow {
            label: "Job Type: ",
            data: todo!(), // match params.job_info.job_desc.task_desc {
                           //     JobTaskDescription::Array { .. } => "Array".into(),
                           //     JobTaskDescription::Graph { .. } => "Graph".into(),
                           // },
        },
        JobInfoDataRow {
            label: "Creation Time: ",
            data: creation_time.format("%b %e, %T").to_string().into(),
        },
        JobInfoDataRow {
            label: "Completion Time: ",
            data: completion_time.unwrap_or_default(),
        },
        JobInfoDataRow {
            label: "Num Tasks: ",
            data: todo!(), /*params.job_info.task_ids.len().to_string().into(),*/
        },
        JobInfoDataRow {
            label: "Log Path: ",
            data: todo!(), //log_path.into(),
        },
        JobInfoDataRow {
            label: "Max Fails: ",
            data: todo!(), // params
                           //     .job_info
                           //     .job_desc
                           //     .max_fails
                           //     .map(|fails| fails.to_string())
                           //     .unwrap_or_default()
                           //     .into(),
        },
    ]
}
