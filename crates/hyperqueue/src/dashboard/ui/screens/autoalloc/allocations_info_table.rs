use crate::dashboard::data::timelines::alloc_timeline::{
    get_allocation_status, AllocationInfo, AllocationStatus,
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

#[derive(Default)]
pub struct AllocationInfoTable {
    table: StatefulTable<(AllocationId, AllocationInfo)>,
}

impl AllocationInfoTable {
    pub fn update<'a>(
        &'a mut self,
        allocation_info: impl Iterator<Item = (&'a AllocationId, &'a AllocationInfo)>,
    ) {
        let rows = create_rows(allocation_info);
        self.table.set_items(rows);
    }

    pub fn select_next_allocation(&mut self) {
        self.table.select_next_wrap();
    }

    pub fn select_previous_allocation(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn clear_selection(&mut self) {
        self.table.clear_selection();
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame, table_style: Style) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Allocations <2>",
                table_headers: Some(vec![
                    "Allocation ID",
                    "#Workers",
                    "Queued Time",
                    "Start Time",
                    "Finish Time",
                ]),
                column_widths: vec![
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                ],
            },
            |(alloc_id, info_row)| {
                let queued_time: DateTime<Local> = info_row.queued_time.into();
                let queued_time_string = queued_time.format("%b %e, %T").to_string();
                let start_time = info_row
                    .start_time
                    .map(|time| {
                        let start_time: DateTime<Local> = time.into();
                        start_time.format("%b %e, %T").to_string()
                    })
                    .unwrap_or_else(|| "".to_string());
                let finish_time = info_row
                    .finish_time
                    .map(|time| {
                        let end_time: DateTime<Local> = time.into();
                        end_time.format("%b %e, %T").to_string()
                    })
                    .unwrap_or_else(|| "".to_string());

                Row::new(vec![
                    Cell::from(alloc_id.as_str()),
                    Cell::from(info_row.worker_count.to_string()),
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
) -> Vec<(AllocationId, AllocationInfo)> {
    let mut info_rows: Vec<(AllocationId, AllocationInfo)> = allocations
        .map(|(alloc_id, info)| (alloc_id.clone(), *info))
        .collect();

    info_rows.sort_by_key(|(_, alloc_info_row)| {
        let status_index = match get_allocation_status(alloc_info_row, SystemTime::now()) {
            AllocationStatus::Running => 0,
            AllocationStatus::Queued => 1,
            AllocationStatus::Finished => 2,
        };
        match alloc_info_row.start_time {
            None => (status_index, alloc_info_row.queued_time),
            Some(start_time) => (status_index, start_time),
        }
    });

    info_rows
}
