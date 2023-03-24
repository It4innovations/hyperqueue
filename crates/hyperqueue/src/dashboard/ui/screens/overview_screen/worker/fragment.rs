use termion::event::Key;

use crate::dashboard::ui::styles::{
    style_footer, style_header_text, table_style_deselected, table_style_selected,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::data::timelines::job_timeline::TaskInfo;
use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screens::overview_screen::worker::cpu_util_table::{
    get_column_constraints, render_cpu_util_table,
};
use crate::dashboard::ui::screens::overview_screen::worker::utilization_chart::WorkerUtilizationChart;
use crate::dashboard::ui::screens::overview_screen::worker::worker_config_table::WorkerConfigTable;
use crate::dashboard::ui::widgets::tasks_table::TasksTable;
use crate::JobTaskId;
use tako::WorkerId;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct WorkerOverviewFragment {
    /// The worker info screen shows data for this worker
    worker_id: Option<WorkerId>,
    utilization_history: WorkerUtilizationChart,
    worker_config_table: WorkerConfigTable,
    worker_tasks_table: TasksTable,

    worker_per_core_cpu_util: Vec<f64>,
}

impl WorkerOverviewFragment {
    pub fn clear_worker_id(&mut self) {
        self.worker_id = None;
    }

    pub fn set_worker_id(&mut self, worker_id: WorkerId) {
        self.worker_id = Some(worker_id);
    }
}

impl WorkerOverviewFragment {
    pub fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = WorkerFragmentLayout::new(&in_area);
        draw_text(
            format!(
                "Details for Worker {}",
                self.worker_id.unwrap_or_default().as_num()
            )
            .as_str(),
            layout.header,
            frame,
            style_header_text(),
        );
        draw_text("<backspace>: Back", layout.footer, frame, style_footer());

        let cpu_usage_columns = get_column_constraints(
            layout.current_utilization,
            self.worker_per_core_cpu_util.len(),
        );
        render_cpu_util_table(
            &self.worker_per_core_cpu_util,
            layout.current_utilization,
            frame,
            &cpu_usage_columns,
            table_style_deselected(),
        );

        self.utilization_history
            .draw(layout.utilization_history, frame);

        self.worker_tasks_table.draw(
            "Tasks On Worker",
            layout.tasks,
            frame,
            table_style_selected(),
        );
        self.worker_config_table.draw(layout.configuration, frame);
    }

    pub fn update(&mut self, data: &DashboardData) {
        if let Some(worker_id) = self.worker_id {
            self.utilization_history.update(data, worker_id);

            if let Some(cpu_util) = data
                .workers()
                .get_worker_overview_at(worker_id, data.current_time())
                .and_then(|overview| overview.item.hw_state.as_ref())
                .map(|hw_state| &hw_state.state.cpu_usage.cpu_per_core_percent_usage)
            {
                self.worker_per_core_cpu_util = cpu_util.into_iter().map(|&v| v as f64).collect();
            }

            let tasks_info: Vec<(JobTaskId, &TaskInfo)> =
                data.query_task_history_for_worker(worker_id).collect();
            self.worker_tasks_table.update(tasks_info);

            if let Some(configuration) = data.workers().get_worker_config_for(&worker_id) {
                self.worker_config_table.update(configuration);
            }
        }
    }

    /// Handles key presses for the components of the screen
    pub fn handle_key(&mut self, key: Key) {
        match key {
            Key::Down => self.worker_tasks_table.select_next_task(),
            Key::Up => self.worker_tasks_table.select_previous_task(),
            Key::Backspace => self.worker_tasks_table.clear_selection(),

            _ => {}
        }
    }
}

/**
*  __________________________
   |--------Header---------|
   |       Cpu Util        |
   |-----------------------|
   |     Worker Info       |
   |-----------------------|
   |--------Footer---------|
   |-----------------------|
 **/
struct WorkerFragmentLayout {
    header: Rect,
    utilization_history: Rect,
    current_utilization: Rect,
    tasks: Rect,
    configuration: Rect,
    footer: Rect,
}

impl WorkerFragmentLayout {
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
            .constraints(vec![Constraint::Percentage(40), Constraint::Percentage(60)])
            .direction(Direction::Horizontal)
            .split(base_chunks[1]);

        let bottom_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(70), Constraint::Percentage(30)])
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
