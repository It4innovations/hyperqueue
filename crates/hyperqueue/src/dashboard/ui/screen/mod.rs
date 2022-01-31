use termion::event::Key;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::controller::ScreenController;
use crate::dashboard::ui::terminal::DashboardFrame;

pub mod controller;

pub trait Screen {
    fn draw(&mut self, frame: &mut DashboardFrame);
    fn update(&mut self, data: &DashboardData, controller: &mut ScreenController);
    fn handle_key(&mut self, key: Key, controller: &mut ScreenController);
}
