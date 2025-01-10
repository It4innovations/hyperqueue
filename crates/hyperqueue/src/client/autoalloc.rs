use crate::server::autoalloc::AllocationState;

impl AllocationState {
    pub fn is_failed(&self) -> bool {
        match self {
            AllocationState::Finished {
                disconnected_workers,
                ..
            } => disconnected_workers.all_crashed(),
            AllocationState::FinishedUnexpectedly { failed, .. } => *failed,
            AllocationState::Queued | AllocationState::Running { .. } => false,
        }
    }
}
