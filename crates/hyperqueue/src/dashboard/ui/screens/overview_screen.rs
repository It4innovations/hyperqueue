use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::overview::fragment::ClusterOverviewFragment;
use crate::dashboard::ui::fragments::worker::fragment::WorkerOverviewFragment;
use crate::dashboard::ui::screen::controller::ScreenController;
use crate::dashboard::ui::screen::{
    Fragment, FromFragmentMessage, Screen, ScreenTab, ToFragmentMessage,
};
use crate::dashboard::ui::terminal::DashboardFrame;
use tako::WorkerId;
use termion::event::Key;
use tui::layout::Rect;

pub struct OverviewScreen {
    overview_fragments: Vec<(ScreenTab, Box<dyn Fragment>)>,
    active_fragment: usize,

    selected_worker_id: Option<WorkerId>,
}

impl Screen for OverviewScreen {
    fn get_tabs(&self) -> &(Vec<ScreenTab>, usize) {
        todo!()
    }

    fn draw(&mut self, in_area: Rect, frame: &mut DashboardFrame) {
        self.get_active_fragment().unwrap().draw(in_area, frame);
    }

    fn update(&mut self, data: &DashboardData, controller: &mut ScreenController) {
        if let Some(worker_id) = self.selected_worker_id {
            self.overview_fragments
                .get_mut(1)
                .unwrap()
                .1
                .handle_message(ToFragmentMessage::SetWorkerId(worker_id))
        }
        self.get_active_fragment().unwrap().update(data, controller);
    }

    fn handle_key(&mut self, key: Key, controller: &mut ScreenController) {
        match key {
            Key::Right => self.switch_to_worker_overview_tab(),
            Key::Left => {
                if self.active_fragment == 0 {
                    controller.show_auto_allocator_screen()
                } else {
                    self.switch_to_cluster_overview_tab()
                }
            }
            _ => {
                if let Some(FromFragmentMessage::WorkerIdChanged(worker_id)) = self
                    .get_active_fragment()
                    .unwrap()
                    .handle_key(key, controller)
                {
                    self.selected_worker_id = Some(worker_id);
                }
            }
        }
    }
}

impl OverviewScreen {
    fn get_active_fragment(&mut self) -> Option<&mut Box<dyn Fragment>> {
        self.overview_fragments
            .get_mut(self.active_fragment)
            .map(|(_, frag)| frag)
    }

    fn switch_to_worker_overview_tab(&mut self) {
        if self.selected_worker_id.is_some() {
            self.active_fragment = 1;
        }
    }

    fn switch_to_cluster_overview_tab(&mut self) {
        self.active_fragment = 0;
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
            selected_worker_id: None,
        }
    }
}
