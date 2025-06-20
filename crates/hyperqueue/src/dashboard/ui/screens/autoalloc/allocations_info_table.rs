use crate::dashboard::data::timelines::alloc_timeline::{
    AllocationInfo, AllocationStatus, get_allocation_status,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::server::autoalloc::AllocationId;
use chrono::{DateTime, Local};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Cell, Row};
use std::time::SystemTime;

pub struct AllocationInfoTable {
    table: StatefulTable<(AllocationId, AllocationInfo, AllocationStatus)>,
}

impl Default for AllocationInfoTable {
    fn default() -> Self {
        let mut table = StatefulTable::default();
        table.set_missing_data_label("No allocation queue selected.");
        Self { table }
    }
}

impl AllocationInfoTable {
    pub fn update<'a>(
        &'a mut self,
        allocations: Option<impl Iterator<Item = (&'a AllocationId, &'a AllocationInfo)>>,
        current_time: SystemTime,
    ) {
        let rows = match allocations {
            Some(allocations) => create_rows(allocations, current_time),
            None => vec![],
        };
        self.table.set_items(rows);
    }

    fn select_next_allocation(&mut self) {
        self.table.select_next_wrap();
    }

    fn select_previous_allocation(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame, table_style: Style) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Allocations <2>",
                table_headers: Some(vec![
                    "Allocation ID",
                    "Status",
                    "#Workers",
                    "Queued Time",
                    "Start Time",
                    "Finish Time",
                ]),
                column_widths: vec![
                    Constraint::Percentage(25),
                    Constraint::Percentage(20),
                    Constraint::Percentage(10),
                    Constraint::Percentage(15),
                    Constraint::Percentage(15),
                    Constraint::Percentage(15),
                ],
            },
            |(alloc_id, info, status)| {
                let queued_time: DateTime<Local> = info.queued_time.into();
                let queued_time_string = queued_time.format("%b %e, %T").to_string();
                let start_time = info
                    .start_time
                    .map(|time| {
                        let start_time: DateTime<Local> = time.into();
                        start_time.format("%b %e, %T").to_string()
                    })
                    .unwrap_or_else(|| "".to_string());
                let finish_time = info
                    .finish_time
                    .map(|time| {
                        let end_time: DateTime<Local> = time.into();
                        end_time.format("%b %e, %T").to_string()
                    })
                    .unwrap_or_else(|| "".to_string());

                let status = match status {
                    AllocationStatus::Missing => "<missing>",
                    AllocationStatus::Queued => "queued",
                    AllocationStatus::Running => "running",
                    AllocationStatus::Finished => "finished",
                };
                Row::new(vec![
                    Cell::from(alloc_id.as_str()),
                    Cell::from(status),
                    Cell::from(info.worker_count.to_string()),
                    Cell::from(queued_time_string),
                    Cell::from(start_time),
                    Cell::from(finish_time),
                ])
            },
            table_style,
        );
    }
    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => self.select_next_allocation(),
            KeyCode::Up => self.select_previous_allocation(),
            _ => {}
        }
    }
}

fn create_rows<'a>(
    allocations: impl Iterator<Item = (&'a AllocationId, &'a AllocationInfo)>,
    time: SystemTime,
) -> Vec<(AllocationId, AllocationInfo, AllocationStatus)> {
    let mut info_rows: Vec<(AllocationId, AllocationInfo, AllocationStatus)> = allocations
        .map(|(alloc_id, info)| (alloc_id.clone(), *info, get_allocation_status(info, time)))
        .filter(|(_, _, status)| !status.is_missing())
        .collect();

    info_rows.sort_by_key(|(_, info, status)| {
        let index = match status {
            AllocationStatus::Running => 0,
            AllocationStatus::Queued => 1,
            AllocationStatus::Finished => 2,
            AllocationStatus::Missing => 3,
        };
        match info.start_time {
            None => (index, info.queued_time),
            Some(start_time) => (index, start_time),
        }
    });

    info_rows
}
