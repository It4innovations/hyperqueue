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

    #[inline]
    pub fn bulk_reserve(&mut self, count: u64) -> u64 {
        let value = self.value;
        self.value += count;
        value
    }

    #[inline]
    pub fn is_used(&self, id: u64) -> bool {
        id < self.value
    }
}
