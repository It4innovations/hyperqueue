use std::collections::BTreeMap;

use ratatui::layout::Rect;
use ratatui::widgets::canvas::Canvas;

use crate::dashboard::data::timelines::alloc_timeline::{
    AllocationInfo, AllocationStatus, get_allocation_status,
};
use crate::dashboard::data::{DashboardData, TimeRange};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::FilledRectangle;
use crate::dashboard::ui::widgets::chart::get_time_as_secs;
use crate::server::autoalloc::{AllocationId, QueueId};
use ratatui::style::Color;
use ratatui::symbols;
use ratatui::widgets::{Block, Borders};
use std::default::Default;

/// Into how many parts do we split the available horizontal time X axis.
const HORIZONTAL_SPLIT: u64 = 100;

/// Draws horizontal lines (one line per allocation), where the color of the line shows the
/// status of the allocation.
#[derive(Default)]
pub struct AllocationsChart {
    allocation_records: BTreeMap<(QueueId, AllocationId), AllocationInfo>,
    range: TimeRange,
}

impl AllocationsChart {
    pub fn update(&mut self, data: &DashboardData, selected_queue: Option<QueueId>) {
        self.range = data.current_time_range();

        let queues = match selected_queue {
            Some(queue) => vec![queue],
            None => data
                .query_allocation_queues_at(self.range.end())
                .map(|(id, _)| *id)
                .collect(),
        };

        let allocations = queues
            .iter()
            .filter_map(|queue_id| {
                data.query_allocations_info(*queue_id).map(|allocations| {
                    allocations.map(|(id, info)| ((*queue_id, id.clone()), *info))
                })
            })
            .flatten()
            .collect::<BTreeMap<(QueueId, AllocationId), AllocationInfo>>();
        self.allocation_records = allocations;
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let start = get_time_as_secs(self.range.start());
        let end = get_time_as_secs(self.range.end());
        let width = end - start;

        let height = 10;

        let canvas = Canvas::default()
            .marker(symbols::Marker::Block)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Allocation Timeline"),
            )
            .paint(|ctx| {
                let mut y_pos: f64 = 0.0;

                let rect_height = 0.9; // padding
                let rect_width = width / HORIZONTAL_SPLIT as f64;

                for ((queue_id, alloc_id), info) in self.allocation_records.iter().take(height) {
                    ctx.print(0.0, y_pos, format!("{queue_id}/{alloc_id}"));
                    for time in self.range.generate_steps(HORIZONTAL_SPLIT) {
                        let status = get_allocation_status(info, time);
                        let color = match status {
                            AllocationStatus::Missing => continue,
                            AllocationStatus::Queued => Color::Yellow,
                            AllocationStatus::Running => Color::Green,
                            AllocationStatus::Finished => Color::Blue,
                        };
                        // Normalize time
                        let time = get_time_as_secs(time) - start;
                        ctx.draw(&FilledRectangle {
                            x: time,
                            y: y_pos,
                            width: rect_width,
                            height: rect_height,
                            color,
                        });
                    }
                    y_pos += 1.0;
                }
            })
            .x_bounds([0.0, end - start])
            .y_bounds([0.0, height as f64]);
        frame.render_widget(canvas, rect);
    }
}
