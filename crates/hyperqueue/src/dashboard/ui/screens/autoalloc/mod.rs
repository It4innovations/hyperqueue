use crate::dashboard::data::DashboardData;
use crate::dashboard::data::timelines::alloc_timeline::AllocationQueueInfo;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::autoalloc::alloc_timeline_chart::AllocationsChart;
use crate::dashboard::ui::screens::autoalloc::allocations_info_table::AllocationInfoTable;
use crate::dashboard::ui::screens::autoalloc::queue_info_table::AllocationQueueInfoTable;
use crate::dashboard::ui::screens::autoalloc::queue_params_display::QueueParamsTable;
use crate::dashboard::ui::styles::{style_footer, table_style_deselected, table_style_selected};
use crate::dashboard::ui::terminal::DashboardFrame;
use crate::dashboard::ui::widgets::text::draw_text;
use crate::server::autoalloc::QueueId;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Direction, Layout, Rect};

mod alloc_timeline_chart;
mod allocations_info_table;
mod queue_info_table;
mod queue_params_display;

#[derive(Default)]
pub struct AutoAllocScreen {
    queue_info_table: AllocationQueueInfoTable,
    queue_params_table: QueueParamsTable,
    allocations_info_table: AllocationInfoTable,
    allocations_chart: AllocationsChart,

    component_in_focus: FocusedComponent,
}

#[derive(Default)]
enum FocusedComponent {
    #[default]
    QueueInfoTable,
    AllocationInfoTable,
}

impl Screen for AutoAllocScreen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        let layout = AutoAllocFragmentLayout::new(&in_area);

        let (queues_table_style, allocations_table_style) = match self.component_in_focus {
            FocusedComponent::QueueInfoTable => (table_style_selected(), table_style_deselected()),
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
            "<1>: Allocation Queues, <2>: Allocations, <ESC>: Clear selected allocation",
            layout.footer_chunk,
            frame,
            style_footer(),
        );
    }

    fn update(&mut self, data: &DashboardData) {
        let queue_infos: Vec<(&QueueId, &AllocationQueueInfo)> = data
            .query_allocation_queues_at(data.current_time())
            .collect();
        self.queue_info_table.update(queue_infos);

        if let Some(selected) = self.queue_info_table.get_selected_queue() {
            self.allocations_chart.update(data, selected);
        }

        self.queue_params_table.update(
            self.queue_info_table
                .get_selected_queue()
                .and_then(|selected| data.query_allocation_params(selected)),
        );

        self.allocations_info_table.update(
            self.queue_info_table
                .get_selected_queue()
                .and_then(|selected| data.query_allocations_info_at(selected, data.current_time())),
            data.current_time(),
        );
    }

    fn handle_key(&mut self, key: KeyEvent) {
        match self.component_in_focus {
            FocusedComponent::QueueInfoTable => self.queue_info_table.handle_key(key),
            FocusedComponent::AllocationInfoTable => self.allocations_info_table.handle_key(key),
        };

        match key.code {
            KeyCode::Char('1') => {
                self.component_in_focus = FocusedComponent::QueueInfoTable;
            }
            KeyCode::Char('2') => self.component_in_focus = FocusedComponent::AllocationInfoTable,
            KeyCode::Esc => self.queue_info_table.clear_selection(),
            _ => {}
        }
    }
}

/**
*  __________________________
   |        Chart          |
   |-----------------------|
   |  queues  |            |
   |----------| alloc_info |
   | q_params |            |
   |________Footer_________|
 **/
struct AutoAllocFragmentLayout {
    chart_chunk: Rect,
    allocation_queue_params_chunk: Rect,
    queue_info_chunk: Rect,
    allocation_info_chunk: Rect,
    footer_chunk: Rect,
}

impl AutoAllocFragmentLayout {
    fn new(rect: &Rect) -> Self {
        let auto_alloc_screen_chunks = Layout::default()
            .constraints(vec![
                Constraint::Percentage(40),
                Constraint::Percentage(55),
                Constraint::Percentage(5),
            ])
            .direction(Direction::Vertical)
            .split(*rect);

        let component_area = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Horizontal)
            .margin(0)
            .split(auto_alloc_screen_chunks[1]);

        let queue_info_area = Layout::default()
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .direction(Direction::Vertical)
            .split(component_area[0]);

        Self {
            chart_chunk: auto_alloc_screen_chunks[0],
            queue_info_chunk: queue_info_area[0],
            allocation_queue_params_chunk: queue_info_area[1],
            allocation_info_chunk: component_area[1],
            footer_chunk: auto_alloc_screen_chunks[2],
        }
    }
}
