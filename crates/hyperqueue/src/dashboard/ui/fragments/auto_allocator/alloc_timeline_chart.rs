use std::time::{Duration, SystemTime};

use tako::common::Map;
use tui::layout::Rect;
use tui::widgets::canvas::{Canvas, Context, Painter, Shape};

use crate::dashboard::data::alloc_timeline::{get_allocation_status, AllocationStatus};
use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::server::autoalloc::{AllocationId, QueueId};
use chrono::{DateTime, Local};
use std::default::Default;
use tui::style::{Color, Style};
use tui::symbols;
use tui::text::Span;
use tui::widgets::{Block, Borders};

/// Margin for the chart time labels.
const LABEL_Y_MARGIN: f64 = 1.00;
/// Margin between the alloc timeline and its `allocation_id` label.
const LINE_Y_MARGIN: f64 = 0.10;
/// Space taken by string = `num_chars` * `CHAR_SPACE_FACTOR`.
const CHAR_SPACE_FACTOR: f64 = 2.30;
/// Space taken by time labels in chart `HH:MM`.
const TIME_CHAR_COUNT: f64 = 5.00 * CHAR_SPACE_FACTOR;

/// Stores the allocation status at a point in time.
struct AllocationInfoPoint {
    state: AllocationStatus,
    time: SystemTime,
}

struct AllocationsChartData {
    /// The status of an allocation at different points in time.
    allocation_records: Map<AllocationId, Vec<AllocationInfoPoint>>,
    /// Max time that is being shown currently.
    end_time: SystemTime,
    /// The size of the viewing window.
    view_size: Duration,
}

#[derive(Default)]
pub struct AllocationsChart {
    chart_data: AllocationsChartData,
}

impl AllocationsChart {
    pub fn update(
        &mut self,
        data: &DashboardData,
        query_descriptor: QueueId,
        display_time: SystemTime,
    ) {
        self.chart_data.end_time = display_time;

        let mut query_time = self.chart_data.end_time - self.chart_data.view_size;
        let mut query_times = vec![];
        while query_time <= self.chart_data.end_time {
            query_times.push(query_time);
            query_time += Duration::from_secs(1);
        }

        let mut allocation_history: Vec<(AllocationId, AllocationInfoPoint)> = vec![];
        query_times.iter().for_each(|query_time| {
            if let Some(alloc_map) = data.query_allocations_info_at(query_descriptor, *query_time) {
                let points = alloc_map
                    .filter(|(_, alloc_info)| {
                        let finished_before_query = alloc_info
                            .finish_time
                            .map(|finish_time| finish_time < *query_time)
                            .unwrap_or(false);

                        !finished_before_query
                    })
                    .map(|(alloc_id, alloc_info)| {
                        (
                            alloc_id.clone(),
                            AllocationInfoPoint {
                                state: get_allocation_status(alloc_info, *query_time),
                                time: *query_time,
                            },
                        )
                    });
                allocation_history.extend(points);
            }
        });

        self.chart_data.allocation_records.clear();
        allocation_history.into_iter().for_each(|(id, point)| {
            if let Some(points) = self.chart_data.allocation_records.get_mut(&id) {
                points.push(point);
            } else {
                self.chart_data.allocation_records.insert(id, vec![point]);
            }
        });
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        let canvas = Canvas::default()
            .marker(symbols::Marker::Block)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Alloc Timeline"),
            )
            .paint(|ctx| {
                ctx.draw(&self.chart_data);
                self.draw_labels(ctx);
            })
            .x_bounds([0.0, self.chart_data.view_size.as_secs_f64()])
            .y_bounds([
                0.0,
                self.chart_data.allocation_records.len() as f64 + LABEL_Y_MARGIN,
            ]);
        frame.render_widget(canvas, rect);
    }
}

impl Shape for AllocationsChartData {
    /// Draws the allocation_queue_timelines
    fn draw(&self, painter: &mut Painter) {
        let mut y_pos: f64 = LABEL_Y_MARGIN;
        for (_, alloc_info_pt) in &self.allocation_records {
            alloc_info_pt.iter().for_each(|point| {
                let x_pos = self.view_size.as_secs_f64()
                    - self
                        .end_time
                        .duration_since(point.time)
                        .unwrap_or_default()
                        .as_secs_f64();
                if let Some((x, y)) = painter.get_point(x_pos, y_pos) {
                    painter.paint(
                        x,
                        y,
                        match point.state {
                            AllocationStatus::Queued => Color::Yellow,
                            AllocationStatus::Running => Color::Green,
                            _ => unreachable!(),
                        },
                    );
                }
            });
            y_pos += LABEL_Y_MARGIN;
        }
    }
}

impl AllocationsChart {
    /// Adds the x_axis labels for time and `allocation_id` label to each timeline.
    fn draw_labels(&self, ctx: &mut Context) {
        let begin_time: DateTime<Local> =
            (self.chart_data.end_time - self.chart_data.view_size).into();
        let end_time: DateTime<Local> = self.chart_data.end_time.into();

        ctx.print(
            0.0,
            0.0,
            Span::styled(
                begin_time.format("%H:%M").to_string(),
                Style::default().fg(Color::Yellow),
            ),
        );
        ctx.print(
            self.chart_data.view_size.as_secs_f64() - TIME_CHAR_COUNT,
            0.0,
            Span::styled(
                end_time.format("%H:%M").to_string(),
                Style::default().fg(Color::Yellow),
            ),
        );
        // Margin to prevent overlap with time labels and timeline plots.
        let mut y_pos: f64 = LABEL_Y_MARGIN + LINE_Y_MARGIN;
        // Inserts the allocation_id labels for each of the allocation timelines.
        self.chart_data
            .allocation_records
            .iter()
            .for_each(|(alloc_id, _)| {
                ctx.print(
                    0.00,
                    y_pos,
                    Span::styled(alloc_id.clone(), Style::default().fg(Color::Yellow)),
                );
                y_pos += LABEL_Y_MARGIN;
            });
    }
}

impl Default for AllocationsChartData {
    fn default() -> Self {
        Self {
            allocation_records: Default::default(),
            end_time: SystemTime::now(),
            view_size: Duration::from_secs(600),
        }
    }
}
