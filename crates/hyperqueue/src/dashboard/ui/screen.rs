use crate::dashboard::ui::terminal::DashboardFrame;

pub trait Screen {
    fn draw(&self, frame: &mut DashboardFrame);
}
