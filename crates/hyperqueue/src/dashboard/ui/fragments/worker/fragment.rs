use std::time::SystemTime;
use termion::event::Key;

use crate::dashboard::ui::styles::{
    style_footer, style_header_text, table_style_deselected, table_style_selected,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::data::job_timeline::TaskInfo;
use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::worker::cpu_util_table::{
    get_column_constraints, render_cpu_util_table,
};
use crate::dashboard::ui::fragments::worker::worker_config_table::WorkerConfigTable;
use crate::dashboard::ui::widgets::tasks_table::TasksTable;
use crate::JobTaskId;
use tako::WorkerId;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct WorkerOverviewFragment {
    /// The worker info screen shows data for this worker
    worker_id: Option<WorkerId>,
    worker_info_table: WorkerConfigTable,
    worker_tasks_table: TasksTable,

    worker_per_core_cpu_util: Vec<f32>,
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
            layout.header_chunk,
            frame,
            style_header_text(),
        );
        draw_text(
            "<backspace>: Back",
            layout.footer_chunk,
            frame,
            style_footer(),
        );

        let cpu_usage_columns = get_column_constraints(
            layout.worker_util_chunk,
            self.worker_per_core_cpu_util.len(),
        );
        render_cpu_util_table(
            &self.worker_per_core_cpu_util,
            layout.worker_util_chunk,
            frame,
            &cpu_usage_columns,
            table_style_deselected(),
        );

        self.worker_tasks_table.draw(
            "Tasks On Worker",
            layout.tasks_table_chunk,
            frame,
            table_style_selected(),
        );
        self.worker_info_table
            .draw(layout.worker_info_table_chunk, frame);
    }

    pub fn update(&mut self, data: &DashboardData) {
        if let Some(worker_id) = self.worker_id.and_then(|worker_id| {
            data.query_connected_worker_ids(SystemTime::now())
                .find(|connected_id| *connected_id == worker_id)
        }) {
            // Update CPU Util table.
            if let Some(cpu_util) = data
                .query_worker_overview_at(worker_id, SystemTime::now())
                .and_then(|overview| overview.hw_state.as_ref())
                .map(|hw_state| &hw_state.state.cpu_usage.cpu_per_core_percent_usage)
            {
                self.worker_per_core_cpu_util = cpu_util.clone()
            }
            // Update Tasks Table
            let tasks_info: Vec<(JobTaskId, &TaskInfo)> =
                data.query_task_history_for_worker(worker_id).collect();
            self.worker_tasks_table.update(tasks_info);
            // Update Worker Configuration Information
            if let Some(configuration) = data.query_worker_info_for(&worker_id) {
                self.worker_info_table.update(configuration);
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
    header_chunk: Rect,
    tasks_table_chunk: Rect,
    worker_util_chunk: Rect,
    worker_info_table_chunk: Rect,
    footer_chunk: Rect,
}

impl WorkerFragmentLayout {
    fn new(rect: &Rect) -> Self {
        let base_chunks = tui::layout::Layout::default()
            .constraints(vec![
                Constraint::Percentage(5),
                Constraint::Percentage(50),
                Constraint::Percentage(40),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        let info_chunks = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(base_chunks[1]);

        Self {
            header_chunk: base_chunks[0],
            worker_util_chunk: info_chunks[0],
            worker_info_table_chunk: info_chunks[1],
            tasks_table_chunk: base_chunks[2],
            footer_chunk: base_chunks[3],
        }
    }
}
