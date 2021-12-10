use crate::dashboard::data::DashboardData;
use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::home::screen::HomeScreen;
use tako::common::WrappedRcRefCell;

pub struct DashboardState {
    screens: Vec<Box<dyn Screen>>,
    current_screen: usize, // TODO: usize newtype wrapper or enum
    data_source: WrappedRcRefCell<DashboardData>,
}

impl DashboardState {
    pub fn new(data_source: DashboardData) -> Self {
        Self {
            data_source: WrappedRcRefCell::wrap(data_source),
            screens: vec![Box::new(HomeScreen::default())],
            current_screen: 0,
        }
    }

    pub fn get_data_source(&self) -> &WrappedRcRefCell<DashboardData> {
        &self.data_source
    }

    pub fn get_current_screen_mut(&mut self) -> &mut dyn Screen {
        self.screens[self.current_screen].as_mut()
    }
}
