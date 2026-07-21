use crate::internal::scheduler::gap::GapCache;
use crate::{Map, ResourceVariantId, TaskId, WorkerId};
use std::time::Duration;

pub struct SchedulerConfig {
    /// The number of tasks that are never prefilled (wrt a given resource request)
    /// In other words, tasks above this limit are part of prefilling.
    pub proactive_filling_reserve: u32,
    /// The maximal number of tasks that are prefilled per worker.
    /// TODO: Maybe we can choose it dynamically wrt. resources of workers
    ///       but small tens looks reasonable
    pub proactive_filling_max: u32,
    /// Hard wall-clock cap on a single scheduler MILP solve. An emergency
    /// backstop, not a tuning knob: on expiry, the best incumbent found so
    /// far is dispatched and marked non-optimal.
    pub mip_time_limit: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            proactive_filling_reserve: 16,
            proactive_filling_max: 40,
            mip_time_limit: default_mip_time_limit(),
        }
    }
}

// Unit tests assert exact placement counts on small instances, so default to
// a generous limit. Tests exercising the bounded solve set mip_time_limit
// explicitly via TestEnv::set_scheduler_config.
#[cfg(test)]
fn default_mip_time_limit() -> Duration {
    Duration::from_secs(60)
}

#[cfg(not(test))]
fn default_mip_time_limit() -> Duration {
    Duration::from_secs(5)
}

#[derive(Default)]
pub(crate) struct SchedulerState {
    pub gap_cache: GapCache,
    pub config: SchedulerConfig,
    pub redirects: Map<TaskId, (WorkerId, ResourceVariantId)>,
}
