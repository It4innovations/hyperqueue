use tako::WorkerId;
use termion::event::Key;
use tui::layout::Rect;

use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::controller::ScreenController;
use crate::dashboard::ui::terminal::DashboardFrame;

pub mod controller;

pub trait Fragment {
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame);
    fn update(&mut self, data: &DashboardData, controller: &mut ScreenController);
    fn handle_message(&mut self, message: ToFragmentMessage);
    fn handle_key(
        &mut self,
        key: Key,
        controller: &mut ScreenController,
    ) -> Option<FromFragmentMessage>;
}

#[derive(Debug, Clone)]
pub struct ScreenTab {
    pub tab_title: String,
    //todo: add keybindings list
}

pub trait Screen {
    fn get_tabs(&self) -> &(Vec<ScreenTab>, usize);
    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame);
    fn update(&mut self, data: &DashboardData, controller: &mut ScreenController);
    fn handle_key(&mut self, key: Key, controller: &mut ScreenController);
}

/// The different messages the screens can send to the fragments to pass data.
pub enum ToFragmentMessage {
    SetWorkerId(WorkerId),
}

/// The messages a fragment can send to a screen upon a key event.
pub enum FromFragmentMessage {
    WorkerIdChanged(WorkerId),
}
