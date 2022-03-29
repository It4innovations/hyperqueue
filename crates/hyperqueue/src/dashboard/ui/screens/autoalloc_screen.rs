use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::fragments::auto_allocator::fragment::AutoAllocatorFragment;
use crate::dashboard::ui::screen::controller::ScreenController;
use crate::dashboard::ui::screen::{Fragment, Screen, ScreenTab};
use crate::dashboard::ui::terminal::DashboardFrame;
use termion::event::Key;
use tui::layout::Rect;

pub struct AutoAllocScreen {
    autoalloc_fragments: Vec<(ScreenTab, Box<dyn Fragment>)>,
    active_fragment: usize,
}

impl Screen for AutoAllocScreen {
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
        match key {
            Key::Right => controller.show_cluster_overview(),
            Key::Left => controller.show_job_screen(),
            _ => {
                self.get_active_fragment()
                    .unwrap()
                    .handle_key(key, controller);
            }
        }
    }
}

impl AutoAllocScreen {
    fn get_active_fragment(&mut self) -> Option<&mut Box<dyn Fragment>> {
        self.autoalloc_fragments
            .get_mut(self.active_fragment)
            .map(|(_, frag)| frag)
    }
}

impl Default for AutoAllocScreen {
    fn default() -> Self {
        let autoalloc_tab = (
            ScreenTab {
                tab_title: "Autoalloc Info".into(),
            },
            Box::new(AutoAllocatorFragment::default()) as Box<dyn Fragment>,
        );

        AutoAllocScreen {
            autoalloc_fragments: vec![autoalloc_tab],
            active_fragment: 0,
        }
    }
}
