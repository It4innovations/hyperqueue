use crate::internal::scheduler::gap_cache::GapCache;
use crate::{Map, ResourceVariantId, TaskId, WorkerId};

#[derive(Default)]
pub(crate) struct SchedulerState {
    pub gap_cache: GapCache,
    pub redirects: Map<TaskId, (WorkerId, ResourceVariantId)>,
}
