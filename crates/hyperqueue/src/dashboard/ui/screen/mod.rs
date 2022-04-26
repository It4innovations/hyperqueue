use std::time::SystemTime;
use termion::event::Key;
use tui::layout::Rect;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;

pub trait Screen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame);
    fn update(&mut self, data: &DashboardData, display_time: SystemTime);
    fn handle_key(&mut self, key: Key);
}
