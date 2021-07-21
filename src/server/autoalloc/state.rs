use std::time::Duration;

pub struct AutoAllocState {
    refresh_interval: Duration,
}

impl AutoAllocState {
    pub fn new(refresh_interval: Duration) -> AutoAllocState {
        Self { refresh_interval }
    }
}

impl AutoAllocState {
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }
}
