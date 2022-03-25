use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::job::fragment::JobFragment;
use crate::dashboard::ui::fragments::worker::fragment::WorkerOverviewFragment;
use crate::dashboard::ui::screen::controller::ScreenController;
use crate::dashboard::ui::screen::{Fragment, Screen, ScreenTab};
use crate::dashboard::ui::terminal::DashboardFrame;
use termion::event::Key;
use tui::layout::Rect;

pub struct JobScreen {
    job_fragments: Vec<(ScreenTab, Box<dyn Fragment>)>,
    active_fragment: usize,
}

impl Screen for JobScreen {
    fn get_tabs(&self) -> &(Vec<ScreenTab>, usize) {
        todo!()
    }

    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        self.get_active_fragment().unwrap().draw(in_area, frame);
    }

    fn update(&mut self, data: &DashboardData, controller: &mut ScreenController) {
        self.get_active_fragment().unwrap().update(data, controller);
    }

    fn handle_key(&mut self, key: Key, controller: &mut ScreenController) {
        self.get_active_fragment()
            .unwrap()
            .handle_key(key, controller);
    }
}

impl JobScreen {
    fn get_active_fragment(&mut self) -> Option<&mut Box<dyn Fragment>> {
        self.job_fragments
            .get_mut(self.active_fragment)
            .map(|(_, frag)| frag)
    }
}

impl Default for JobScreen {
    fn default() -> Self {
        let jobs_tab = (
            ScreenTab {
                tab_title: "Jobs".into(),
            },
            Box::new(JobFragment::default()) as Box<dyn Fragment>,
        );
        let worker_tab = (
            ScreenTab {
                tab_title: "Worker Details".into(),
            },
            Box::new(WorkerOverviewFragment::default()) as Box<dyn Fragment>,
        );

        JobScreen {
            job_fragments: vec![jobs_tab, worker_tab],
            active_fragment: 0,
        }
    }
}
