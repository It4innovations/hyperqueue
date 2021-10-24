use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::home::screen::HomeScreen;

pub struct DashboardState {
    screens: Vec<Box<dyn Screen>>,
    current_screen: usize, // TODO: usize newtype wrapper or enum
}

impl DashboardState {
    pub fn new() -> Self {
        let screens: Vec<Box<dyn Screen>> = vec![Box::new(HomeScreen::new())];
        DashboardState {
            screens,
            current_screen: 0,
        }
    }

    pub fn get_current_screen(&self) -> &dyn Screen {
        self.screens[self.current_screen].as_ref()
    }
}
