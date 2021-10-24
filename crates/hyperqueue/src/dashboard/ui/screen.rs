use tako::messages::gateway::CollectedOverview;
use termion::event::Key;

use crate::dashboard::ui::terminal::DashboardFrame;

pub trait Screen {
    fn draw(&mut self, frame: &mut DashboardFrame);

    // Note: might be generalized in the future to allow the screen to fetch custom data by itself
    fn update(&mut self, overview: CollectedOverview);

    fn handle_key(&mut self, key: Key);
}
