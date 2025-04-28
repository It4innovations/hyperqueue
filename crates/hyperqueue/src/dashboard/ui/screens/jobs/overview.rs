use crate::dashboard::ui::styles::{style_footer, table_style_deselected, table_style_selected};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;
use crossterm::event::{KeyCode, KeyEvent};
use std::default::Default;
use std::time::SystemTime;

use crate::dashboard::data::timelines::job_timeline::TaskInfo;
use crate::dashboard::data::DashboardData;

use crate::dashboard::ui::screens::jobs::jobs_table::JobsTable;
use crate::dashboard::ui::widgets::tasks_table::TasksTable;

use crate::dashboard::ui::screens::jobs::job_info_display::JobInfoTable;
use crate::dashboard::ui::screens::jobs::job_tasks_chart::JobTaskChart;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use tako::JobTaskId;
use tako::WorkerId;

pub struct JobOverview {
    job_task_chart: JobTaskChart,

    job_list: JobsTable,
    job_info_table: JobInfoTable,
    job_tasks_table: TasksTable,

    component_in_focus: FocusedComponent,
}

impl Default for JobOverview {
    fn default() -> Self {
        Self {
            job_task_chart: Default::default(),
            job_list: Default::default(),
            job_info_table: Default::default(),
            job_tasks_table: TasksTable::interactive(),
            component_in_focus: Default::default(),
        }
    }
}

#[derive(Default)]
enum FocusedComponent {
    #[default]
    JobsTable,
    JobTasksTable,
}

impl JobOverview {
    pub fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = JobOverviewLayout::new(&in_area);

        let (jobs_table_style, tasks_table_style) = match self.component_in_focus {
            FocusedComponent::JobsTable => (table_style_selected(), table_style_deselected()),
            FocusedComponent::JobTasksTable => (table_style_deselected(), table_style_selected()),
        };

        self.job_info_table.draw(layout.job_info_chunk, frame);
        self.job_task_chart.draw(layout.chart_chunk, frame);
        self.job_list
            .draw(layout.job_list_chunk, frame, jobs_table_style);
        self.job_tasks_table.draw(
            "Tasks <2>",
            layout.job_tasks_chunk,
            frame,
            true,
            tasks_table_style,
        );

        draw_text(
            "<\u{21F5}> select, <1> Jobs, <2> Tasks, <i> worker details for selected task",
            layout.footer_chunk,
            frame,
            style_footer(),
        );
    }

    pub fn update(&mut self, data: &DashboardData) {
        self.job_list.update(data);

        if let Some(job_id) = self.job_list.get_selected_item() {
            let task_infos: Vec<(JobTaskId, &TaskInfo)> = data
                .query_task_history_for_job(job_id, SystemTime::now())
                .collect();
            self.job_tasks_table.update(task_infos);
            self.job_task_chart.set_job_id(job_id);
        } else {
            self.job_task_chart.unset_job_id();
        }
        if let Some(job_info) = self
            .job_list
            .get_selected_item()
            .and_then(|job_id| data.query_job_info_for_job(job_id))
        {
            self.job_info_table.update(job_info);
        }

        self.job_task_chart.update(data);
    }

    /// Handles key presses for the components of the screen
    pub fn handle_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('1') => {
                self.component_in_focus = FocusedComponent::JobsTable;
                self.job_tasks_table.clear_selection();
            }
            KeyCode::Char('2') => self.component_in_focus = FocusedComponent::JobTasksTable,
            _ => {}
        }
        match self.component_in_focus {
            FocusedComponent::JobsTable => self.job_list.handle_key(key),
            FocusedComponent::JobTasksTable => self.job_tasks_table.handle_key(key),
        };
    }

    pub fn get_selected_task(&self) -> Option<(JobTaskId, WorkerId)> {
        self.job_tasks_table.get_selected_item()
    }
}

/// _____________________________
/// | Job details |  Task chart |
/// |---------------------------|
/// |  Job list |  Task list    |
/// |---------------------------|
/// |          Footer           |
/// |---------------------------|
struct JobOverviewLayout {
    chart_chunk: Rect,
    job_info_chunk: Rect,
    job_list_chunk: Rect,
    job_tasks_chunk: Rect,
    footer_chunk: Rect,
}

impl JobOverviewLayout {
    fn new(rect: &Rect) -> Self {
        let job_screen_chunks = Layout::default()
            .constraints(vec![
                Constraint::Percentage(45),
                Constraint::Percentage(50),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        let graph_and_details_area = Layout::default()
            .constraints(vec![Constraint::Percentage(35), Constraint::Percentage(65)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(job_screen_chunks[0]);

        let table_area = Layout::default()
            .constraints(vec![Constraint::Percentage(30), Constraint::Percentage(80)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(job_screen_chunks[1]);

        Self {
            job_info_chunk: graph_and_details_area[0],
            chart_chunk: graph_and_details_area[1],
            job_list_chunk: table_area[0],
            job_tasks_chunk: table_area[1],
            footer_chunk: job_screen_chunks[2],
        }
    }
}
