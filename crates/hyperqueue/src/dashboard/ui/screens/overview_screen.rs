use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::overview::fragment::ClusterOverviewFragment;
use crate::dashboard::ui::fragments::worker::fragment::WorkerOverviewFragment;
use crate::dashboard::ui::screen::controller::ScreenController;
use crate::dashboard::ui::screen::{Fragment, Screen, ScreenTab};
use crate::dashboard::ui::terminal::DashboardFrame;
use termion::event::Key;
use tui::layout::Rect;

pub struct OverviewScreen {
    overview_fragments: Vec<(ScreenTab, Box<dyn Fragment>)>,
    active_fragment: usize,
}

impl Screen for OverviewScreen {
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

impl OverviewScreen {
    fn get_active_fragment(&mut self) -> Option<&mut Box<dyn Fragment>> {
        self.overview_fragments
            .get_mut(self.active_fragment)
            .map(|(_, frag)| frag)
    }
}

impl Default for OverviewScreen {
    fn default() -> Self {
        let overview_tab = (
            ScreenTab {
                tab_title: "Overview".into(),
            },
            Box::new(ClusterOverviewFragment::default()) as Box<dyn Fragment>,
        );
        let worker_tab = (
            ScreenTab {
                tab_title: "Worker Details".into(),
            },
            Box::new(WorkerOverviewFragment::default()) as Box<dyn Fragment>,
        );

        OverviewScreen {
            overview_fragments: vec![overview_tab, worker_tab],
            active_fragment: 0,
        }
    }
}
