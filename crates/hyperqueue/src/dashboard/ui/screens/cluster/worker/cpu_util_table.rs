use crate::common::format::human_size;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Cell, Row, Table};
use std::cmp;
use tako::hwstats::MemoryStats;

use crate::dashboard::ui::styles;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::progressbar::{
    ProgressPrintStyle, get_progress_bar_color, render_progress_bar_at,
};
use crate::dashboard::utils::calculate_average;

const CPU_METER_PROGRESSBAR_WIDTH: u8 = 18;
// 4 characters for the label
const CPU_METER_WIDTH: u8 = CPU_METER_PROGRESSBAR_WIDTH + 4;

pub fn render_cpu_util_table(
    cpu_util_list: &[f64],
    mem_util: &MemoryStats,
    rect: Rect,
    frame: &mut DashboardFrame,
    table_style: Style,
) {
    if cpu_util_list.is_empty() {
        return;
    }
    let constraints = get_column_constraints(rect, cpu_util_list.len());

    let width = constraints.len();
    let height = (cpu_util_list.len() as f64 / width as f64).ceil() as usize;

    let mut rows: Vec<Vec<(f64, usize)>> = vec![vec![]; height];
    for (position, &cpu_util) in cpu_util_list.iter().enumerate() {
        let row = position % height;
        rows[row].push((cpu_util, position));
    }

    let rows: Vec<Row> = rows
        .into_iter()
        .map(|targets| {
            let columns: Vec<Cell> = targets
                .into_iter()
                .map(|(cpu_util, position)| {
                    let progress = cpu_util / 100.00;
                    Cell::from(render_progress_bar_at(
                        Some(format!("{position:>3} ")),
                        progress,
                        CPU_METER_PROGRESSBAR_WIDTH,
                        ProgressPrintStyle::default(),
                    ))
                    .style(get_progress_bar_color(progress))
                })
                .collect();
            Row::new(columns)
        })
        .collect();

    let avg_cpu = calculate_average(cpu_util_list);

    let mem_used = mem_util.total - mem_util.free;
    let title = styles::table_title(format!(
        "Worker Utilization ({} CPUs), Avg CPU = {:.0}%, Mem = {:.0}% ({}/{})",
        cpu_util_list.len(),
        avg_cpu,
        (mem_used as f64 / mem_util.total as f64) * 100.0,
        human_size(mem_used),
        human_size(mem_util.total)
    ));
    let body_block = styles::table_block_with_title(title);

    let table = Table::new(rows, constraints)
        .block(body_block)
        .row_highlight_style(styles::style_table_highlight())
        .style(table_style);

    frame.render_widget(table, rect);
}

/// Creates the column sizes for the cpu_util_table, each column divides the row equally.
fn get_column_constraints(rect: Rect, num_cpus: usize) -> Vec<Constraint> {
    let max_columns = (rect.width / CPU_METER_WIDTH as u16) as usize;
    let num_columns = cmp::min(max_columns, num_cpus);

    std::iter::repeat_n(
        Constraint::Percentage((100 / num_columns) as u16),
        num_columns,
    )
    .collect()
}
