use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::terminal::DashboardFrame;
use crossterm::event::KeyEvent;
use fragment::AutoAllocatorFragment;
use ratatui::layout::Rect;

mod alloc_timeline_chart;
mod allocations_info_table;
mod fragment;
mod queue_info_table;
mod queue_params_display;

#[derive(Default)]
pub struct AutoAllocScreen {
    auto_allocator_fragment: AutoAllocatorFragment,
}

impl Screen for AutoAllocScreen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        self.auto_allocator_fragment.draw(in_area, frame);
    }

    fn update(&mut self, data: &DashboardData) {
        self.auto_allocator_fragment.update(data);
    }

    fn handle_key(&mut self, key: KeyEvent) {
        self.auto_allocator_fragment.handle_key(key);
    }
}
