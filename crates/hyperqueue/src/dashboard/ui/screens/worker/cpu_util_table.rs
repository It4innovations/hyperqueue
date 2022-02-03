use std::cmp;
use tui::layout::{Constraint, Rect};
use tui::widgets::{Cell, Row, Table};

use crate::dashboard::ui::styles;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::progressbar::{
    get_progress_bar_color, render_progress_bar_at, ProgressPrintStyle,
};

const CPU_METER_LEN: u8 = 18;

pub fn render_cpu_util_table(
    cpu_util_list: &[f32],
    rect: Rect,
    frame: &mut DashboardFrame,
    constraints: &[Constraint],
) {
    let indexed_util: Vec<(&f32, i32)> = cpu_util_list.iter().zip(1..).collect();
    let rows: Vec<Row> = indexed_util
        .chunks(constraints.len())
        .map(|cpu_util_row| {
            let columns: Vec<Cell> = cpu_util_row
                .iter()
                .map(|(&cpu_util, position)| {
                    let progress = cpu_util / 100.00;
                    Cell::from(render_progress_bar_at(
                        Some(format!("{:>3} ", position)),
                        progress,
                        CPU_METER_LEN,
                        ProgressPrintStyle::default(),
                    ))
                    .style(get_progress_bar_color(progress))
                })
                .collect();
            Row::new(columns)
        })
        .collect();

    let avg_prog = cpu_util_list
        .iter()
        .copied()
        .reduce(|cpu_a, cpu_b| (cpu_a + cpu_b))
        .unwrap_or(0.0)
        / cpu_util_list.len() as f32;
    let avg_progressbar = render_progress_bar_at(
        None,
        avg_prog / 100.00,
        CPU_METER_LEN,
        ProgressPrintStyle::default(),
    );
    let title = styles::table_title(format!(
        "Worker CPU Utilization ({} CPUs), Avg = {}",
        cpu_util_list.len(),
        avg_progressbar
    ));
    let body_block = styles::table_block_with_title(title);

    let table = Table::new(rows)
        .block(body_block)
        .highlight_style(styles::style_table_highlight())
        .style(styles::style_table_row())
        .widths(constraints);

    frame.render_widget(table, rect);
}

/// Creates the column sizes for the cpu_util_table, each column divides the row equally.
pub fn get_column_constraints(rect: Rect, num_cpus: usize) -> Vec<Constraint> {
    let max_columns = (rect.width / CPU_METER_LEN as u16) as usize;
    let num_columns = cmp::min(max_columns, num_cpus);

    std::iter::repeat(Constraint::Percentage((100 / num_columns) as u16))
        .take(num_columns)
        .collect()
}
