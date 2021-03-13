#[derive(Default, Debug)]
pub struct IdCounter {
    value: u64,
}

impl IdCounter {
    #[inline]
    pub fn next(&mut self) -> u64 {
        let value = self.value;
        self.value += 1;
        value
    }
}
