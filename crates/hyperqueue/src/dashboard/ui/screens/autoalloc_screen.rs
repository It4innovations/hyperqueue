use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::auto_allocator::fragment::AutoAllocatorFragment;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::terminal::DashboardFrame;
use std::time::SystemTime;
use termion::event::Key;
use tui::layout::Rect;

#[derive(Default)]
pub struct AutoAllocScreen {
    auto_allocator_fragment: AutoAllocatorFragment,
}

impl Screen for AutoAllocScreen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        self.auto_allocator_fragment.draw(in_area, frame);
    }

    fn update(&mut self, data: &DashboardData, display_time: SystemTime) {
        self.auto_allocator_fragment.update(data, display_time);
    }

    fn handle_key(&mut self, key: Key) {
        self.auto_allocator_fragment.handle_key(key);
    }
}
