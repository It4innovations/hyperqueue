use crate::common::format::human_size;
use crate::dashboard::data::DashboardData;
use itertools::Itertools;
use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Cell, Row, Table};
use std::cmp;
use tako::hwstats::MemoryStats;
use tako::resources::{
    CPU_RESOURCE_NAME, ResourceDescriptorItem, ResourceDescriptorKind, ResourceIndex,
};
use tako::worker::WorkerConfiguration;
use tako::{Set, WorkerId};

use crate::dashboard::ui::styles::{self, table_style_deselected};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::progressbar::{
    ProgressPrintStyle, get_cpu_progress_bar_color, render_progress_bar_at,
};
use crate::dashboard::utils::calculate_average;

const CPU_METER_PROGRESSBAR_WIDTH: u8 = 18;
// 4 characters for the label
const CPU_METER_WIDTH: u8 = CPU_METER_PROGRESSBAR_WIDTH + 4;

#[derive(Default)]
pub struct CpuUtilTable {
    utilization: Option<Utilization>,
    cpu_view_mode: CpuViewMode,
    cpu_state: Option<WorkerCpuState>,
}

#[derive(Default, PartialEq)]
pub enum CpuViewMode {
    Global,
    #[default]
    WorkerManaged,
    WorkerAssigned,
}

pub enum CpuScope {
    /// Worker manages all of the known Node cpus
    Node,
    /// Worker manages only a subset of known Node cpus
    Subset,
}

impl CpuViewMode {
    fn next(&mut self, cpu_scope: CpuScope) {
        match cpu_scope {
            CpuScope::Node => {
                *self = match self {
                    CpuViewMode::WorkerManaged => CpuViewMode::WorkerAssigned,
                    CpuViewMode::WorkerAssigned => CpuViewMode::WorkerManaged,
                    CpuViewMode::Global => CpuViewMode::WorkerManaged, // To skip out of the global in case the state changes
                }
            }
            CpuScope::Subset => {
                *self = match self {
                    CpuViewMode::Global => CpuViewMode::WorkerManaged,
                    CpuViewMode::WorkerManaged => CpuViewMode::WorkerAssigned,
                    CpuViewMode::WorkerAssigned => CpuViewMode::Global,
                }
            }
        }
    }

    fn next_text(&self, cpu_scope: CpuScope) -> &str {
        match cpu_scope {
            CpuScope::Node => {
                match self {
                    CpuViewMode::WorkerManaged => "Show worker assigned CPU utilization",
                    CpuViewMode::WorkerAssigned => "Show worker managed CPU utilization",
                    CpuViewMode::Global => "Show worker managed CPU utilization", // To skip out of the global in case the state changes
                }
            }
            CpuScope::Subset => match self {
                CpuViewMode::Global => "Show worker managed CPU utilization",
                CpuViewMode::WorkerManaged => "Show worker assigned CPU utilization",
                CpuViewMode::WorkerAssigned => "Show global CPU utilization",
            },
        }
    }

    fn get_visible_indices(
        &self,
        total_cpus: usize,
        cpu_state: &WorkerCpuState,
    ) -> Set<ResourceIndex> {
        match self {
            CpuViewMode::Global => (0..total_cpus)
                .map(|idx| ResourceIndex::new(idx as u32))
                .collect(),
            CpuViewMode::WorkerManaged => cpu_state.managed_cpus.clone(),
            CpuViewMode::WorkerAssigned => cpu_state.assigned_cpus.clone(),
        }
    }

    fn set_default(&mut self) {
        *self = CpuViewMode::WorkerManaged;
    }
}

struct Utilization {
    cpu: Vec<f64>,
    memory: MemoryStats,
}

struct WorkerCpuState {
    /// CPU cores currently managed by the worker.
    managed_cpus: Set<ResourceIndex>,
    /// CPU cores assigned to at least a single task that is currently running on this worker.
    assigned_cpus: Set<ResourceIndex>,
}

impl CpuUtilTable {
    pub fn update(
        &mut self,
        data: &DashboardData,
        worker_id: WorkerId,
        worker_config: Option<&WorkerConfiguration>,
    ) {
        if let Some(configuration) = worker_config {
            let managed_cpus: Option<&ResourceDescriptorItem> = configuration
                .resources
                .resources
                .iter()
                .find(|resource| resource.name == CPU_RESOURCE_NAME);

            let managed_cpus = if let Some(managed_cpus) = managed_cpus {
                cpu_resource_desc_to_idx(managed_cpus).unwrap_or_default()
            } else {
                Set::default()
            };

            if let Some(overview) = data
                .workers()
                .query_worker_overview_at(worker_id, data.current_time())
            {
                let assigned_cpus: Set<ResourceIndex> = match self.cpu_view_mode {
                    CpuViewMode::WorkerManaged | CpuViewMode::WorkerAssigned => overview
                        .item
                        .running_tasks
                        .iter()
                        .flat_map(|(_id, task_resource_alloc)| {
                            task_resource_alloc
                                .resources
                                .iter()
                                .filter_map(|resource_alloc| {
                                    if resource_alloc.resource == CPU_RESOURCE_NAME {
                                        Some(resource_alloc.indices.iter().map(|(index, _)| *index))
                                    } else {
                                        None
                                    }
                                })
                        })
                        .flatten()
                        .collect(),
                    CpuViewMode::Global => Set::default(),
                };

                self.cpu_state = Some(WorkerCpuState {
                    managed_cpus,
                    assigned_cpus,
                });

                if let Some(hw_state) = overview.item.hw_state.as_ref() {
                    self.utilization = Some(Utilization {
                        cpu: hw_state
                            .state
                            .cpu_usage
                            .cpu_per_core_percent_usage
                            .iter()
                            .map(|&v| v as f64)
                            .collect(),
                        memory: hw_state.state.memory_usage.clone(),
                    })
                }
            } else {
                self.cpu_state = None;
            }
        } else {
            self.cpu_state = None;
        }
    }

    pub fn draw(&mut self, rect: Rect, frame: &mut DashboardFrame) {
        if let (Some(util), Some(cpu_state)) = (&self.utilization, &self.cpu_state) {
            if util.cpu.is_empty() {
                return;
            }

            let visible_indices = self
                .cpu_view_mode
                .get_visible_indices(util.cpu.len(), cpu_state);

            let cell_data: Vec<(u32, f64, bool)> = visible_indices
                .into_iter()
                .map(|idx| {
                    let val = util
                        .cpu
                        .get(idx.as_num() as usize)
                        .copied()
                        .unwrap_or_default();
                    let is_used = cpu_state.assigned_cpus.contains(&idx);
                    (idx.as_num(), val, is_used)
                })
                .sorted_by_key(|&(idx, _, used)| (std::cmp::Reverse(used), idx))
                .collect();

            let constraints = get_column_constraints(rect, cell_data.len());

            let width = constraints.len();

            let total_cells = cell_data.len();
            let rows: Vec<Row> = if width > 0 && total_cells > 0 {
                let num_rows = total_cells.div_ceil(width);

                (0..num_rows)
                    .map(|row_start_idx| {
                        let cells: Vec<Cell> = cell_data
                            .iter()
                            .skip(row_start_idx)
                            .step_by(num_rows)
                            .map(|(id, cpu_util, used)| {
                                let progress = cpu_util / 100.0;
                                let style = get_cpu_progress_bar_color(
                                    progress,
                                    *used,
                                    &self.cpu_view_mode,
                                );

                                Cell::from(render_progress_bar_at(
                                    Some(format!("{id:>3} ")),
                                    progress,
                                    CPU_METER_PROGRESSBAR_WIDTH,
                                    ProgressPrintStyle::default(),
                                ))
                                .style(style)
                            })
                            .collect();

                        Row::new(cells)
                    })
                    .collect()
            } else {
                vec![]
            };

            let mem_used = util.memory.total - util.memory.free;
            let (which_util, num_cpus, avg_cpu) =
                create_title_info(&self.cpu_view_mode, &cell_data);

            let title = styles::table_title(format!(
                "{} Utilization ({} CPUs), Avg CPU = {:.0}%, Mem = {:.0}% ({}/{})",
                which_util,
                num_cpus,
                avg_cpu,
                (mem_used as f64 / util.memory.total as f64) * 100.0,
                human_size(mem_used),
                human_size(util.memory.total)
            ));
            let body_block = styles::table_block_with_title(title);

            let table = Table::new(rows, constraints)
                .block(body_block)
                .row_highlight_style(styles::style_table_highlight())
                .style(table_style_deselected());

            frame.render_widget(table, rect);
        }
    }

    pub fn next_view(&mut self) {
        let scope = self.get_current_scope();
        self.cpu_view_mode.next(scope);
    }

    pub fn next_text(&mut self) -> &str {
        let scope = self.get_current_scope();
        self.cpu_view_mode.next_text(scope)
    }

    pub fn clear_table(&mut self) {
        self.clear_util();
        self.set_default_view();
    }

    fn clear_util(&mut self) {
        self.utilization = None;
    }

    fn set_default_view(&mut self) {
        self.cpu_view_mode.set_default();
    }

    fn get_current_scope(&self) -> CpuScope {
        if let (Some(util), Some(cpu_state)) = (&self.utilization, &self.cpu_state) {
            if util.cpu.len() == cpu_state.managed_cpus.len() {
                CpuScope::Node
            } else {
                CpuScope::Subset
            }
        } else {
            CpuScope::Node
        }
    }
}

/// Creates the column sizes for the cpu_util_table, each column divides the row equally.
fn get_column_constraints(rect: Rect, num_cpus: usize) -> Vec<Constraint> {
    let max_columns = (rect.width / CPU_METER_WIDTH as u16) as usize;
    let num_columns = cmp::min(max_columns, num_cpus);

    if num_columns > 0 {
        std::iter::repeat_n(
            Constraint::Percentage((100 / num_columns) as u16),
            num_columns,
        )
        .collect()
    } else {
        vec![]
    }
}

fn create_title_info(
    util_render_mode: &CpuViewMode,
    cpu_table_data: &[(u32, f64, bool)],
) -> (String, usize, f64) {
    let which_util = match util_render_mode {
        CpuViewMode::Global => "Node",
        CpuViewMode::WorkerManaged => "Worker Managed",
        CpuViewMode::WorkerAssigned => "Worker Assigned",
    }
    .to_string();

    let num_cpus = cpu_table_data.len();
    let cpu_utils = cpu_table_data
        .iter()
        .map(|(_, util, _)| *util)
        .collect::<Vec<f64>>();
    let avg_usage = calculate_average(&cpu_utils);

    (which_util, num_cpus, avg_usage)
}

/// Mapping of CPU resource descriptor item to set of Resource Indexes
fn cpu_resource_desc_to_idx(resource: &ResourceDescriptorItem) -> Option<Set<ResourceIndex>> {
    match &resource.kind {
        ResourceDescriptorKind::List { values } => values
            .iter()
            .map(|s| s.parse::<ResourceIndex>())
            .collect::<Result<Set<_>, _>>()
            .ok(),
        ResourceDescriptorKind::Range { start, end } => Some(
            (u32::from(*start)..=u32::from(*end))
                .map(ResourceIndex::from)
                .collect(),
        ),
        ResourceDescriptorKind::Groups { groups } => Some(
            groups
                .iter()
                .flat_map(|group| group.iter())
                .map(|s| s.parse::<ResourceIndex>())
                .collect::<Result<Set<_>, _>>()
                .ok()?,
        ),
        // Based on Resource kind `sum` cannot be used with CPUs. CPUs must have identity
        ResourceDescriptorKind::Sum { .. } => unreachable!(),
    }
}
