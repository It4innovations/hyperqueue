use crate::dashboard::data::timelines::alloc_timeline::AllocationQueueInfo;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::table::{StatefulTable, TableColumnHeaders};
use crate::server::autoalloc::QueueId;
use chrono::{DateTime, Local};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Cell, Row};

#[derive(Default)]
pub struct AllocationQueueInfoTable {
    table: StatefulTable<QueueInfoRow>,
}

impl AllocationQueueInfoTable {
    pub fn update(&mut self, queue_infos: Vec<(&QueueId, &AllocationQueueInfo)>) {
        let rows = create_rows(queue_infos);
        self.table.set_items(rows);
    }

    pub fn select_next_queue(&mut self) {
        self.table.select_next_wrap();
    }

    pub fn select_previous_queue(&mut self) {
        self.table.select_previous_wrap();
    }

    pub fn get_selected_queue(&self) -> Option<QueueId> {
        let selection = self.table.current_selection();
        selection.map(|row| row.queue_id)
    }

    pub fn clear_selection(&mut self) {
        self.table.clear_selection();
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame, table_style: Style) {
        self.table.draw(
            rect,
            frame,
            TableColumnHeaders {
                title: "Allocation Queues <1>",
                table_headers: Some(vec![
                    "Descriptor ID",
                    "#Allocations",
                    "Creation Time",
                    "Removal Time",
                ]),
                column_widths: vec![
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                ],
            },
            |data| {
                Row::new(vec![
                    Cell::from(data.queue_id.to_string()),
                    Cell::from(data.num_allocations.to_string()),
                    Cell::from(data.creation_time.as_str()),
                    Cell::from(data.removal_time.as_str()),
                ])
            },
            table_style,
        );
    }

    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => self.select_next_queue(),
            KeyCode::Up => self.select_previous_queue(),
            _ => {}
        }
    }
}

struct QueueInfoRow {
    queue_id: QueueId,
    num_allocations: u32,
    creation_time: String,
    removal_time: String,
}

fn create_rows(mut queues: Vec<(&QueueId, &AllocationQueueInfo)>) -> Vec<QueueInfoRow> {
    queues.sort_by_key(|(_, queue_info)| queue_info.creation_time);
    queues
        .iter()
        .map(|(queue_id, info)| {
            let creation_time: DateTime<Local> = info.creation_time.into();
            let removal_time = info
                .removal_time
                .map(|time| {
                    let end_time: DateTime<Local> = time.into();
                    end_time.format("%b %e, %T").to_string()
                })
                .unwrap_or_else(|| "".to_string());

            QueueInfoRow {
                queue_id: **queue_id,
                num_allocations: info.allocations.len() as u32,
                creation_time: creation_time.format("%b %e, %T").to_string(),
                removal_time,
            }
        })
        .collect()
}
