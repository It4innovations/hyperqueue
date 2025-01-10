use std::time::Duration;

/// How many directories of completed allocations should be kept on disk across all allocation
/// queues.
pub const MAX_KEPT_DIRECTORIES: usize = 20;

/// If no autoalloc messages arrive after this duration, queues will be refreshed.
pub fn get_refresh_timeout() -> Duration {
    get_duration_from_env("HQ_AUTOALLOC_REFRESH_INTERVAL_MS")
        .unwrap_or_else(|| Duration::from_secs(5 * 60))
}

/// Minimum time between successive status (e.g. qstat) checks.
pub fn get_status_check_interval() -> Duration {
    get_duration_from_env("HQ_AUTOALLOC_STATUS_CHECK_INTERVAL_MS")
        .unwrap_or_else(|| Duration::from_secs(30 * 60))
}

fn get_duration_from_env(key: &str) -> Option<Duration> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
}

/// Maximum number of successive allocation submission failures permitted
/// before the allocation queue will be paused.
pub const MAX_SUBMISSION_FAILS: u64 = 10;
/// Maximum number of successive allocation execution failures permitted
/// before the allocation queue will be paused.
pub fn max_allocation_fails() -> u64 {
    std::env::var("HQ_AUTOALLOC_MAX_ALLOCATION_FAILS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(3)
}
/// Delay levels between submisisons. See [`super::state::RateLimiter`].
pub const SUBMISSION_DELAYS: [Duration; 5] = [
    Duration::ZERO,
    Duration::from_secs(60),
    Duration::from_secs(15 * 60),
    Duration::from_secs(30 * 60),
    Duration::from_secs(60 * 60),
];

/// Maximum number of status errors that can be received for a queued allocation
/// before that allocation will be assumed to be finished.
pub const MAX_QUEUED_STATUS_ERROR_COUNT: u32 = 10;

/// Maximum number of status errors that can be received for a running allocation
/// before that allocation will be assumed to be finished.
///
/// We don't want to end running allocations too quickly, so we give a bit more leeway here than
/// for queued allocations.
pub const MAX_RUNNING_STATUS_ERROR_COUNT: u32 = 20;
