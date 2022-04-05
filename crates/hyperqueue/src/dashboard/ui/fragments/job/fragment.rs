use std::default::Default;
use std::time::SystemTime;
use termion::event::Key;

use crate::dashboard::ui::styles::{
    style_footer, style_header_text, table_style_deselected, table_style_selected,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::data::job_timeline::TaskInfo;
use crate::dashboard::data::DashboardData;

use crate::dashboard::ui::fragments::job::jobs_table::JobsTable;
use crate::dashboard::ui::widgets::tasks_table::TasksTable;

use crate::dashboard::ui::fragments::job::job_info_display::JobInfoTable;
use crate::dashboard::ui::fragments::job::job_tasks_chart::JobTaskChart;
use crate::TakoTaskId;
use tako::WorkerId;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct JobFragment {
    job_task_chart: JobTaskChart,

    jobs_list: JobsTable,
    job_info_table: JobInfoTable,
    job_tasks_table: TasksTable,

    component_in_focus: FocusedComponent,
}

enum FocusedComponent {
    JobsTable,
    JobTasksTable,
}

impl JobFragment {
    pub fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = JobFragmentLayout::new(&in_area);
        draw_text("Job Info", layout.header_chunk, frame, style_header_text());

        let (jobs_table_style, tasks_table_style) = match self.component_in_focus {
            FocusedComponent::JobsTable => (table_style_selected(), table_style_deselected()),
            FocusedComponent::JobTasksTable => (table_style_deselected(), table_style_selected()),
        };

        self.job_info_table.draw(layout.job_info_chunk, frame);
        self.job_task_chart.draw(layout.chart_chunk, frame);
        self.jobs_list
            .draw(layout.job_list_chunk, frame, tasks_table_style);
        self.job_tasks_table.draw(
            "Started Tasks <2>",
            layout.job_tasks_chunk,
            frame,
            jobs_table_style,
        );

        draw_text(
            "<\u{21F5}> select, <1> Jobs, <2> Started Tasks, <i> worker details for selected task",
            layout.footer_chunk,
            frame,
            style_footer(),
        );
    }

    pub fn update(&mut self, data: &DashboardData) {
        self.jobs_list.update(data);

        if let Some(job_id) = self.jobs_list.get_selected_item() {
            let task_infos: Vec<(&TakoTaskId, &TaskInfo)> = data
                .query_task_history_for_job(job_id, SystemTime::now())
                .collect();
            self.job_tasks_table.update(task_infos);
            self.job_task_chart.set_job_id(job_id);
        } else {
            self.job_task_chart.clear_chart();
        }
        if let Some(job_info) = self
            .jobs_list
            .get_selected_item()
            .and_then(|job_id| data.query_job_info_for_job(job_id))
        {
            self.job_info_table.update(job_info);
        }

        self.job_task_chart.update(data);
    }

    /// Handles key presses for the components of the screen
    pub fn handle_key(&mut self, key: Key) {
        match key {
            Key::Char('1') => {
                self.component_in_focus = FocusedComponent::JobsTable;
                self.job_tasks_table.clear_selection();
            }
            Key::Char('2') => self.component_in_focus = FocusedComponent::JobTasksTable,
            _ => {}
        }
        match self.component_in_focus {
            FocusedComponent::JobsTable => self.jobs_list.handle_key(key),
            FocusedComponent::JobTasksTable => self.job_tasks_table.handle_key(key),
        };
    }

    pub fn get_selected_task(&self) -> Option<(TakoTaskId, WorkerId)> {
        self.job_tasks_table.get_selected_item()
    }
}

/**
*  _________________________
   |--------Header---------|
   |        Chart          |
   |-----------------------|
   |  j_info  |   j_tasks  |
   |________Footer_________|
 **/
struct JobFragmentLayout {
    header_chunk: Rect,
    chart_chunk: Rect,
    job_info_chunk: Rect,
    job_list_chunk: Rect,
    job_tasks_chunk: Rect,
    footer_chunk: Rect,
}

impl JobFragmentLayout {
    fn new(rect: &Rect) -> Self {
        let job_screen_chunks = tui::layout::Layout::default()
            .constraints(vec![
                Constraint::Percentage(5),
                Constraint::Percentage(40),
                Constraint::Percentage(50),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        let graph_and_details_area = Layout::default()
            .constraints(vec![Constraint::Percentage(35), Constraint::Percentage(65)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(job_screen_chunks[1]);

        let table_area = Layout::default()
            .constraints(vec![Constraint::Percentage(20), Constraint::Percentage(80)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(job_screen_chunks[2]);

        Self {
            header_chunk: job_screen_chunks[0],
            job_info_chunk: graph_and_details_area[0],
            chart_chunk: graph_and_details_area[1],
            job_list_chunk: table_area[0],
            job_tasks_chunk: table_area[1],
            footer_chunk: job_screen_chunks[3],
        }
    }
}

impl Default for FocusedComponent {
    fn default() -> Self {
        FocusedComponent::JobsTable
    }
}
