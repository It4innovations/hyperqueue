use crate::common::format::human_duration;
use crate::dashboard::data::timelines::job_timeline::DashboardJobInfo;
use crate::dashboard::ui::styles::table_style_deselected;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::transfer::messages::{JobTaskDescription, TaskKind};
use chrono::{DateTime, Local};
use itertools::Itertools;
use ratatui::layout::{Constraint, Rect};
use ratatui::text::Text;
use ratatui::widgets::{Cell, Paragraph, Row};
use std::borrow::Cow;
use tako::gateway::ResourceRequestVariants;

#[derive(Default)]
pub struct JobInfoTable {
    table: StatefulTable<JobInfoDataRow>,
}

#[derive(Default, Debug)]
struct JobInfoDataRow {
    label: &'static str,
    data: Cell<'static>,
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
            |data| Row::new(vec![Cell::from(data.label), data.data.clone()]),
            table_style_deselected(),
        );
    }
}
fn create_rows(info: &DashboardJobInfo) -> Vec<JobInfoDataRow> {
    let creation_time: DateTime<Local> = info.job_creation_time.into();
    let completion_time: Option<Cow<'static, str>> = info.completion_date.map(|time| {
        let end_time: DateTime<Local> = time.into();
        end_time.format("%d.%m. %H:%M:%S").to_string().into()
    });

    let mut rows = vec![JobInfoDataRow {
        label: "Job Type: ",
        data: match info.submit_data.task_desc {
            JobTaskDescription::Array { .. } => "Array".into(),
            JobTaskDescription::Graph { .. } => "Graph".into(),
        },
    }];
    if let JobTaskDescription::Array { task_desc, .. } = &info.submit_data.task_desc {
        match &task_desc.kind {
            TaskKind::ExternalProgram(program) => {
                // TODO: wrap text
                rows.push(JobInfoDataRow {
                    label: "Cmdline",
                    data: Cell::from(Text::from(
                        program
                            .program
                            .args
                            .iter()
                            .map(|arg| arg.to_string())
                            .join(" "),
                    )),
                });
                // TODO: print more information about the job
                rows.push(JobInfoDataRow {
                    label: "Workdir",
                    data: program.program.cwd.to_string_lossy().to_string().into(),
                });
                rows.push(JobInfoDataRow {
                    label: "Pin mode",
                    data: program.pin_mode.to_str().into(),
                })
            }
        };
        rows.push(JobInfoDataRow {
            label: "Resources",
            data: format_resources(&task_desc.resources).into(),
        });
        if let Some(time_limit) = task_desc.time_limit {
            rows.push(JobInfoDataRow {
                label: "Foo",
                data: human_duration(chrono::Duration::from_std(time_limit).unwrap()).into(),
            });
        }
    }

    rows.extend([
        JobInfoDataRow {
            label: "Creation Time: ",
            data: creation_time.format("%d.%m. %H:%M:%S").to_string().into(),
        },
        JobInfoDataRow {
            label: "Completion Time: ",
            data: completion_time.unwrap_or_default().into(),
        },
        JobInfoDataRow {
            label: "Num Tasks: ",
            data: info.submit_data.task_desc.task_count().to_string().into(),
        },
        JobInfoDataRow {
            label: "Max Fails: ",
            data: info
                .job
                .max_fails
                .map(|fails| fails.to_string())
                .unwrap_or_default()
                .into(),
        },
    ]);
    rows
}

fn format_resources(resources: &ResourceRequestVariants) -> String {
    resources
        .variants
        .iter()
        .map(|request| {
            request
                .resources
                .iter()
                .map(|entry| format!("{}={}", entry.resource, entry.policy))
                .join(", ")
        })
        .join(" OR ")
}
