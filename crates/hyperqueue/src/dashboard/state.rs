use crate::dashboard::ui::screen::Screen;
use crate::dashboard::ui::screens::home::screen::HomeScreen;

pub struct DashboardState {
    screens: Vec<Box<dyn Screen>>,
    current_screen: usize, // TODO: usize newtype wrapper or enum
}

impl Default for DashboardState {
    fn default() -> Self {
        let screens: Vec<Box<dyn Screen>> = vec![Box::new(HomeScreen::default())];
        DashboardState {
            screens,
            current_screen: 0,
        }
    }
}

impl DashboardState {
    pub fn get_current_screen_mut(&mut self) -> &mut dyn Screen {
        self.screens[self.current_screen].as_mut()
    }
}
