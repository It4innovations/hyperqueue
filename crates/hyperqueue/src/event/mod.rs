pub mod events;
pub mod log;
pub mod storage;

use events::MonitoringEventPayload;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

pub type MonitoringEventId = u32;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MonitoringEvent {
    pub id: MonitoringEventId,
    pub time: SystemTime,
    pub payload: MonitoringEventPayload,
}

impl MonitoringEvent {
    #[inline]
    pub fn id(&self) -> MonitoringEventId {
        self.id
    }

    #[inline]
    pub fn time(&self) -> SystemTime {
        self.time
    }

    #[inline]
    pub fn payload(&self) -> &MonitoringEventPayload {
        &self.payload
    }
}
