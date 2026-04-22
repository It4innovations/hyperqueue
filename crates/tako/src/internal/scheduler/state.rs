use crate::internal::scheduler::gap::GapCache;
use crate::{Map, ResourceVariantId, TaskId, WorkerId};

pub struct SchedulerConfig {
    /// The number of tasks that are never prefilled (wrt a given resource request)
    /// In other words, tasks above this limit are part of prefilling.
    pub proactive_filling_reserve: u32,
    /// The maximal number of tasks that are prefilled per worker.
    /// TODO: Maybe we can choose it dynamically wrt. resources of workers
    ///       but small tens looks reasonable
    pub proactive_filling_max: u32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            proactive_filling_reserve: 16,
            proactive_filling_max: 40,
        }
    }
}

#[derive(Default)]
pub(crate) struct SchedulerState {
    pub gap_cache: GapCache,
    pub config: SchedulerConfig,
    pub redirects: Map<TaskId, (WorkerId, ResourceVariantId)>,
}
