use crossterm::event::KeyEvent;
use ratatui::layout::Rect;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::terminal::DashboardFrame;

pub trait Screen {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame);
    fn update(&mut self, data: &DashboardData);
    fn handle_key(&mut self, key: KeyEvent);
}
