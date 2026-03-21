use crate::dashboard::data::DashboardData;
use crate::dashboard::data::timelines::job_timeline::TaskInfo;
use crate::dashboard::ui::screens::cluster::worker::cpu_util_table::render_cpu_util_table;
use crate::dashboard::ui::screens::cluster::worker::worker_config_table::WorkerConfigTable;
use crate::dashboard::ui::screens::cluster::worker::worker_utilization_chart::WorkerUtilizationChart;
use crate::dashboard::ui::styles::{
    style_footer, style_header_text, table_style_deselected, table_style_selected,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::tasks_table::TasksTable;
use crate::dashboard::ui::widgets::text::draw_text;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use tako::hwstats::MemoryStats;
use tako::resources::{
    CPU_RESOURCE_NAME, ResourceDescriptorItem, ResourceDescriptorKind, ResourceIndex,
};
use tako::{JobTaskId, WorkerId};

mod cpu_util_table;
mod worker_config_table;
mod worker_utilization_chart;

pub struct WorkerDetail {
    /// The worker detail is shown for this worker
    worker_id: Option<WorkerId>,
    utilization_history: WorkerUtilizationChart,
    worker_config_table: WorkerConfigTable,
    worker_tasks_table: TasksTable,

    utilization: Option<Utilization>,
    cpu_view_mode: CpuViewMode,
    cpu_scope: CpuScope,
}

impl Default for WorkerDetail {
    fn default() -> Self {
        Self {
            worker_id: None,
            utilization_history: Default::default(),
            worker_config_table: Default::default(),
            worker_tasks_table: TasksTable::non_interactive(),
            utilization: None,
            cpu_view_mode: CpuViewMode::WorkerManaged,
            cpu_scope: CpuScope::Node,
        }
    }
}

#[derive(PartialEq)]
pub enum CpuViewMode {
    Global,
    WorkerManaged,
    WorkerAssigned,
}

#[derive(Debug)]
pub enum CpuScope {
    Node,
    Subset(Vec<ResourceIndex>),
}

impl CpuScope {
    fn estimate_scope(
        detected_cpus: &mut Vec<ResourceIndex>,
        managed_cpus: Vec<&ResourceDescriptorItem>,
    ) -> Option<CpuScope> {
        let mut all_managed_cpus = vec![];
        for resource in managed_cpus {
            match &resource.kind {
                ResourceDescriptorKind::List { values } => {
                    if let Ok(indices) = values
                        .iter()
                        .map(|s| s.parse::<ResourceIndex>())
                        .collect::<Result<Vec<_>, _>>()
                    {
                        all_managed_cpus.extend(indices);
                    } else {
                        return None;
                    }
                }
                ResourceDescriptorKind::Range { start, end } => {
                    for idx in (u32::from(*start))..=(u32::from(*end)) {
                        all_managed_cpus.push(ResourceIndex::new(idx));
                    }
                }
                ResourceDescriptorKind::Groups { groups } => {
                    for group in groups {
                        if let Ok(indices) = group
                            .iter()
                            .map(|s| s.parse::<ResourceIndex>())
                            .collect::<Result<Vec<_>, _>>()
                        {
                            all_managed_cpus.extend(indices);
                        } else {
                            return None;
                        }
                    }
                }
                // Based on Resource kind `sum` cannot be used with CPUs. CPUs must have identity
                ResourceDescriptorKind::Sum { .. } => unreachable!(),
            }
        }

        detected_cpus.sort();
        all_managed_cpus.sort();

        if *detected_cpus == all_managed_cpus {
            Some(CpuScope::Node)
        } else {
            Some(CpuScope::Subset(all_managed_cpus))
        }
    }
}

impl CpuViewMode {
    fn next(&mut self, cpu_manager_state: &CpuScope) {
        match cpu_manager_state {
            CpuScope::Node => {
                *self = match self {
                    CpuViewMode::WorkerManaged => CpuViewMode::WorkerAssigned,
                    CpuViewMode::WorkerAssigned => CpuViewMode::WorkerManaged,
                    CpuViewMode::Global => CpuViewMode::WorkerManaged, // To skip out of the global in case the state changes
                }
            }
            CpuScope::Subset(_items) => {
                *self = match self {
                    CpuViewMode::Global => CpuViewMode::WorkerManaged,
                    CpuViewMode::WorkerManaged => CpuViewMode::WorkerAssigned,
                    CpuViewMode::WorkerAssigned => CpuViewMode::Global,
                }
            }
        }
    }

    fn next_text(&self, cpu_manager_state: &CpuScope) -> &str {
        match cpu_manager_state {
            CpuScope::Node => {
                match self {
                    CpuViewMode::WorkerManaged => "Show worker assigned CPU utilization",
                    CpuViewMode::WorkerAssigned => "Show worker managed CPU utilization",
                    CpuViewMode::Global => "Show worker managed CPU utilization", // To skip out of the global in case the state changes
                }
            }
            CpuScope::Subset(_items) => match self {
                CpuViewMode::Global => "Show worker managed CPU utilization",
                CpuViewMode::WorkerManaged => "Show worker assigned CPU utilization",
                CpuViewMode::WorkerAssigned => "Show global CPU utilization",
            },
        }
    }
}

struct Utilization {
    cpu: Vec<f64>,
    memory: MemoryStats,
    used_cpus: Vec<ResourceIndex>,
}

impl WorkerDetail {
    pub fn clear_worker_id(&mut self) {
        self.worker_id = None;
        self.utilization = None;
    }

    pub fn set_worker_id(&mut self, worker_id: WorkerId) {
        self.worker_id = Some(worker_id);
    }
}

impl WorkerDetail {
    pub fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        assert!(self.worker_id.is_some());
        let layout = WorkerDetailLayout::new(&in_area);
        draw_text(
            format!("Worker {}", self.worker_id.unwrap_or_default().as_num()).as_str(),
            layout.header,
            frame,
            style_header_text(),
        );

        draw_text(
            format!(
                "<backspace>: Back, <c>: {}",
                self.cpu_view_mode.next_text(&self.cpu_scope)
            )
            .as_str(),
            layout.footer,
            frame,
            style_footer(),
        );

        if let Some(util) = &self.utilization {
            render_cpu_util_table(
                &util.cpu,
                &util.memory,
                &util.used_cpus,
                &self.cpu_view_mode,
                &self.cpu_scope,
                layout.current_utilization,
                frame,
                table_style_deselected(),
            );
        }

        self.utilization_history
            .draw(layout.utilization_history, frame);

        self.worker_tasks_table.draw(
            "Tasks On Worker",
            layout.tasks,
            frame,
            false,
            table_style_selected(),
        );
        self.worker_config_table.draw(layout.configuration, frame);
    }

    pub fn update(&mut self, data: &DashboardData) {
        if let Some(worker_id) = self.worker_id {
            self.utilization_history.update(data, worker_id);
            let mut worker_config = None;

            if let Some(configuration) = data.workers().query_worker_config_for(worker_id) {
                self.worker_config_table.update(configuration);
                worker_config = Some(configuration);
            }

            if let Some(overview) = data
                .workers()
                .query_worker_overview_at(worker_id, data.current_time())
            {
                let worker_used_cpus: Vec<ResourceIndex> = match self.cpu_view_mode {
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
                    CpuViewMode::Global => vec![],
                };

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
                        used_cpus: worker_used_cpus,
                    })
                }

                if let Some(configuration) = worker_config {
                    let managed_cpus: Vec<&ResourceDescriptorItem> = configuration
                        .resources
                        .resources
                        .iter()
                        .filter(|resource| resource.name == CPU_RESOURCE_NAME)
                        .collect();
                    if let Some(hw_state) = overview.item.hw_state.as_ref() {
                        let mut detected_cpus: Vec<ResourceIndex> = hw_state
                            .state
                            .cpu_usage
                            .cpu_per_core_percent_usage
                            .iter()
                            .enumerate()
                            .map(|(idx, _)| ResourceIndex::new(idx as u32))
                            .collect();
                        let cpu_scope = CpuScope::estimate_scope(&mut detected_cpus, managed_cpus);
                        if let Some(cpu_scope) = cpu_scope {
                            self.cpu_scope = cpu_scope;
                        }
                    }
                }
            }

            let tasks_info: Vec<(JobTaskId, &TaskInfo)> =
                data.query_task_history_for_worker(worker_id).collect();
            self.worker_tasks_table
                .update(tasks_info, data.current_time());
        }
    }

    /// Handles key presses for the components of the screen
    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Backspace => self.worker_tasks_table.clear_selection(),
            KeyCode::Char('c') => self.cpu_view_mode.next(&self.cpu_scope),
            _ => self.worker_tasks_table.handle_key(key),
        }
    }
}

/// _________________________
/// |        Header         |
/// |       Cpu Util        |
/// |-----------------------|
/// |     Worker Info       |
/// |-----------------------|
/// |        Footer         |
/// |-----------------------|
struct WorkerDetailLayout {
    header: Rect,
    utilization_history: Rect,
    current_utilization: Rect,
    tasks: Rect,
    configuration: Rect,
    footer: Rect,
}

impl WorkerDetailLayout {
    fn new(rect: &Rect) -> Self {
        let base_chunks = Layout::default()
            .constraints(vec![
                Constraint::Percentage(5),
                Constraint::Percentage(50),
                Constraint::Percentage(40),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        let utilization_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Horizontal)
            .split(base_chunks[1]);

        let bottom_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Horizontal)
            .split(base_chunks[2]);

        Self {
            header: base_chunks[0],
            utilization_history: utilization_chunks[0],
            current_utilization: utilization_chunks[1],
            tasks: bottom_chunks[0],
            configuration: bottom_chunks[1],
            footer: base_chunks[3],
        }
    }
}
