use crate::common::format::human_size;
use crate::dashboard::ui::screens::cluster::worker::UtilizationRenderMode;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Cell, Row, Table};
use std::cmp;
use tako::hwstats::MemoryStats;
use tako::resources::ResourceIndex;

use crate::dashboard::ui::styles;
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::progressbar::{
    ProgressPrintStyle, get_progress_bar_color, get_progress_bar_cpu_color, render_progress_bar_at,
};
use crate::dashboard::utils::calculate_average;

const CPU_METER_PROGRESSBAR_WIDTH: u8 = 18;
// 4 characters for the label
const CPU_METER_WIDTH: u8 = CPU_METER_PROGRESSBAR_WIDTH + 4;

pub fn render_cpu_util_table(
    cpu_util_list: &[f64],
    mem_util: &MemoryStats,
    used_cpus: &[ResourceIndex],
    util_render_mode: &UtilizationRenderMode,
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

    let mut rows: Vec<Vec<(f64, usize, bool)>> = vec![vec![]; height];
    if *util_render_mode == UtilizationRenderMode::Worker {
        rows = get_utilization_sorted_by_usage(cpu_util_list, used_cpus, height)
    } else {
        for (position, &cpu_util) in cpu_util_list.iter().enumerate() {
            let row = position % height;
            let used = used_cpus.contains(&ResourceIndex::new(position as u32));
            rows[row].push((cpu_util, position, used));
        }
    }

    let rows: Vec<Row> = rows
        .into_iter()
        .map(|targets| {
            let columns: Vec<Cell> = targets
                .into_iter()
                .map(|(cpu_util, position, used)| {
                    let progress = cpu_util / 100.00;
                    let style = match util_render_mode {
                        UtilizationRenderMode::Global => get_progress_bar_color(progress),
                        UtilizationRenderMode::Worker => get_progress_bar_cpu_color(progress, used),
                    };

                    Cell::from(render_progress_bar_at(
                        Some(format!("{position:>3} ")),
                        progress,
                        CPU_METER_PROGRESSBAR_WIDTH,
                        ProgressPrintStyle::default(),
                    ))
                    .style(style)
                })
                .collect();
            Row::new(columns)
        })
        .collect();

    let mem_used = mem_util.total - mem_util.free;
    let (which_util, num_cpus, avg_cpu) =
        create_title_info(cpu_util_list, used_cpus, util_render_mode);

    let title = styles::table_title(format!(
        "{} Utilization ({} CPUs), Avg CPU = {:.0}%, Mem = {:.0}% ({}/{})",
        which_util,
        num_cpus,
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

fn get_utilization_sorted_by_usage(
    cpu_util_list: &[f64],
    used_cpus: &[ResourceIndex],
    height: usize,
) -> Vec<Vec<(f64, usize, bool)>> {
    let mut all_cpus: Vec<(f64, usize, bool)> = cpu_util_list
        .iter()
        .enumerate()
        .map(|(position, &cpu_util)| {
            let used = used_cpus.contains(&ResourceIndex::new(position as u32));
            (cpu_util, position, used)
        })
        .collect();

    all_cpus.sort_by_key(|&(_, _, used)| std::cmp::Reverse(used));

    let mut rows: Vec<Vec<(f64, usize, bool)>> = vec![vec![]; height];
    for (index, cpu_data) in all_cpus.into_iter().enumerate() {
        let row = index % height;
        rows[row].push(cpu_data);
    }
    rows
}

fn create_title_info(
    cpu_util_list: &[f64],
    used_cpus: &[ResourceIndex],
    util_render_mode: &UtilizationRenderMode,
) -> (String, usize, f64) {
    let which_util = match util_render_mode {
        UtilizationRenderMode::Global => "Node".to_string(),
        UtilizationRenderMode::Worker => "Worker".to_string(),
    };

    let num_cpus = match util_render_mode {
        UtilizationRenderMode::Global => cpu_util_list.len(),
        UtilizationRenderMode::Worker => used_cpus.len(),
    };

    let avg_usage = match util_render_mode {
        UtilizationRenderMode::Global => calculate_average(cpu_util_list),
        UtilizationRenderMode::Worker => {
            let used_cpu_util_list: Vec<f64> = cpu_util_list
                .iter()
                .enumerate()
                .filter_map(|(idx, utilization)| {
                    if used_cpus.contains(&ResourceIndex::new(idx as u32)) {
                        Some(*utilization)
                    } else {
                        None
                    }
                })
                .collect();
            calculate_average(&used_cpu_util_list)
        }
    };

    (which_util, num_cpus, avg_usage)
}
