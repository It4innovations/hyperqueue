#[derive(Copy, Clone, Default, Debug)]
pub struct IdCounter {
    counter: u32,
}
impl IdCounter {
    pub fn new(initial_value: u32) -> Self {
        Self {
            counter: initial_value,
        }
    }
    pub fn increment(&mut self) -> u32 {
        let value = self.counter;
        self.counter += 1;
        value
    }
}
