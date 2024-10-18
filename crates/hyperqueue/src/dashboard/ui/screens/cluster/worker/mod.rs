use crate::dashboard::data::timelines::job_timeline::TaskInfo;
use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screens::cluster::worker::cpu_util_table::render_cpu_util_table;
use crate::dashboard::ui::screens::cluster::worker::worker_config_table::WorkerConfigTable;
use crate::dashboard::ui::screens::cluster::worker::worker_utilization_chart::WorkerUtilizationChart;
use crate::dashboard::ui::styles::{
    style_footer, style_header_text, table_style_deselected, table_style_selected,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::tasks_table::TasksTable;
use crate::dashboard::ui::widgets::text::draw_text;
use crate::JobTaskId;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use tako::hwstats::MemoryStats;
use tako::WorkerId;

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
}

impl Default for WorkerDetail {
    fn default() -> Self {
        Self {
            worker_id: None,
            utilization_history: Default::default(),
            worker_config_table: Default::default(),
            worker_tasks_table: TasksTable::non_interactive(),
            utilization: None,
        }
    }
}

struct Utilization {
    cpu: Vec<f64>,
    memory: MemoryStats,
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
        draw_text("<backspace>: Back", layout.footer, frame, style_footer());

        if let Some(util) = &self.utilization {
            render_cpu_util_table(
                &util.cpu,
                &util.memory,
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

            if let Some((cpu_util, mem_util)) = data
                .workers()
                .query_worker_overview_at(worker_id, data.current_time())
                .and_then(|overview| overview.item.hw_state.as_ref())
                .map(|hw_state| {
                    (
                        &hw_state.state.cpu_usage.cpu_per_core_percent_usage,
                        &hw_state.state.memory_usage,
                    )
                })
            {
                self.utilization = Some(Utilization {
                    cpu: cpu_util.iter().map(|&v| v as f64).collect(),
                    memory: mem_util.clone(),
                });
            }

            let tasks_info: Vec<(JobTaskId, &TaskInfo)> =
                data.query_task_history_for_worker(worker_id).collect();
            self.worker_tasks_table.update(tasks_info);

            if let Some(configuration) = data.workers().query_worker_config_for(worker_id) {
                self.worker_config_table.update(configuration);
            }
        }
    }

    /// Handles key presses for the components of the screen
    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Backspace => self.worker_tasks_table.clear_selection(),
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
