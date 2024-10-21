use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::auto_allocator::fragment::AutoAllocatorFragment;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::terminal::DashboardFrame;
use crossterm::event::KeyEvent;
use ratatui::layout::Rect;

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
