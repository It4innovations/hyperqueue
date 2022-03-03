use std::default::Default;
use std::time::SystemTime;
use termion::event::Key;

use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::styles::{
    style_footer, style_header_text, table_style_deselected, table_style_selected,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;

use crate::dashboard::data::alloc_timeline::AllocationQueueInfo;
use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::controller::ScreenController;
use crate::dashboard::ui::screens::auto_allocator::alloc_timeline_chart::AllocationsChart;
use crate::dashboard::ui::screens::auto_allocator::allocations_info_table::AllocationInfoTable;
use crate::dashboard::ui::screens::auto_allocator::queue_info_table::AllocationQueueInfoTable;
use crate::dashboard::ui::screens::auto_allocator::queue_params_display::QueueParamsTable;
use crate::server::autoalloc::QueueId;
use tui::layout::{Constraint, Direction, Layout, Rect};

#[derive(Default)]
pub struct AutoAllocatorScreen {
    queue_info_table: AllocationQueueInfoTable,
    queue_params_table: QueueParamsTable,
    allocations_info_table: AllocationInfoTable,
    allocations_chart: AllocationsChart,

    component_in_focus: FocusedComponent,
}

enum FocusedComponent {
    QueueParamsTable,
    AllocationInfoTable,
}

impl Screen for AutoAllocatorScreen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = AutoAllocScreenLayout::new(&in_area);
        draw_text(
            "AutoAlloc Info",
            layout.header_chunk,
            frame,
            style_header_text(),
        );

        let (queues_table_style, allocations_table_style) = match self.component_in_focus {
            FocusedComponent::QueueParamsTable => {
                (table_style_selected(), table_style_deselected())
            }
            FocusedComponent::AllocationInfoTable => {
                (table_style_deselected(), table_style_selected())
            }
        };

        self.allocations_chart.draw(layout.chart_chunk, frame);
        self.queue_info_table
            .draw(layout.queue_info_chunk, frame, queues_table_style);
        self.allocations_info_table.draw(
            layout.allocation_info_chunk,
            frame,
            allocations_table_style,
        );
        self.queue_params_table
            .draw(layout.allocation_queue_params_chunk, frame);

        draw_text(
            "Press <- for Jobs Overview, -> for Cluster Overview",
            layout.footer_chunk,
            frame,
            style_footer(),
        );
    }

    fn update(&mut self, data: &DashboardData, _controller: &mut ScreenController) {
        let queue_infos: Vec<(&QueueId, &AllocationQueueInfo)> =
            data.query_allocation_queues_at(SystemTime::now()).collect();
        self.queue_info_table.update(queue_infos);

        if let Some(descriptor) = self.queue_info_table.get_selected_queue_descriptor() {
            self.allocations_chart.update(data, descriptor);
        }

        if let Some(queue_params) = self
            .queue_info_table
            .get_selected_queue_descriptor()
            .and_then(|queue_id| data.query_allocation_params(queue_id))
        {
            self.queue_params_table.update(queue_params)
        }

        if let Some(allocations_map) = self
            .queue_info_table
            .get_selected_queue_descriptor()
            .and_then(|queue_id| data.query_allocations_info_at(queue_id, SystemTime::now()))
        {
            self.allocations_info_table.update(allocations_map);
        }
    }

    /// Handles key presses for the components of the screen
    fn handle_key(&mut self, key: Key, controller: &mut ScreenController) {
        match self.component_in_focus {
            FocusedComponent::QueueParamsTable => self.queue_info_table.handle_key(key),
            FocusedComponent::AllocationInfoTable => self.allocations_info_table.handle_key(key),
        };

        match key {
            Key::Right => controller.show_cluster_overview(),
            Key::Left => controller.show_job_screen(),
            Key::Char('1') => {
                self.component_in_focus = FocusedComponent::QueueParamsTable;
                self.allocations_info_table.clear_selection();
            }
            Key::Char('2') => self.component_in_focus = FocusedComponent::AllocationInfoTable,
            _ => {}
        }
    }
}

/**
*  __________________________
   |--------Header---------|
   |        Chart          |
   |-----------------------|
   |  queues  |            |
   |----------| alloc_info |
   | q_params |            |
   |________Footer_________|
 **/
struct AutoAllocScreenLayout {
    chart_chunk: Rect,
    allocation_queue_params_chunk: Rect,
    header_chunk: Rect,
    queue_info_chunk: Rect,
    allocation_info_chunk: Rect,
    footer_chunk: Rect,
}

impl AutoAllocScreenLayout {
    fn new(rect: &Rect) -> Self {
        let auto_alloc_screen_chunks = tui::layout::Layout::default()
            .constraints(vec![
                Constraint::Percentage(5),
                Constraint::Percentage(40),
                Constraint::Percentage(50),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        let component_area = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(auto_alloc_screen_chunks[2]);

        let queue_info_area = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Vertical)
            .split(component_area[0]);

        Self {
            chart_chunk: auto_alloc_screen_chunks[1],
            header_chunk: auto_alloc_screen_chunks[0],
            queue_info_chunk: queue_info_area[0],
            allocation_queue_params_chunk: queue_info_area[1],
            allocation_info_chunk: component_area[1],
            footer_chunk: auto_alloc_screen_chunks[3],
        }
    }
}

impl Default for FocusedComponent {
    fn default() -> Self {
        FocusedComponent::QueueParamsTable
    }
}
