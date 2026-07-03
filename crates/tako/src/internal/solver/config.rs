use std::time::Duration;

/// Relative MIP optimality gap: the solver accepts a solution once it is
/// provably within this fraction of the true optimum, instead of always
/// proving exact optimality. Task resource requests are themselves estimates
/// (cpu/memory bucketing), so demanding an exact optimum is false precision;
/// a 10% gap trades a small, usually much smaller in practice, placement
/// suboptimality for a solve that reliably finishes in well under a second on
/// realistic instances instead of stalling the single-threaded server.
pub(crate) fn mip_rel_gap() -> f64 {
    // Unit tests assert exact placement counts/priority behavior on small,
    // fast-solving instances -- a nonzero default gap there would trade
    // correctness-test fidelity for a speedup these tiny instances don't
    // need. A test that wants to exercise gap-tuned behavior specifically
    // uses with_test_solver_config below (thread-local, not process-global
    // env vars, so it can't race with unrelated tests running concurrently
    // on other threads).
    #[cfg(test)]
    if let Some(v) = TEST_REL_GAP_OVERRIDE.with(|c| c.get()) {
        return v;
    }
    #[cfg(test)]
    let default = 0.0;
    #[cfg(not(test))]
    let default = 0.10;

    get_f64_from_env("HQ_SCHEDULER_MIP_REL_GAP").unwrap_or(default)
}

/// Hard wall-clock cap on a single scheduling solve. The solver is otherwise
/// unbounded and can run for minutes to hours on workloads with many distinct
/// resource shapes, blocking the single-threaded server (no heartbeats, no
/// RPCs, no other scheduling) for the entire duration. 5s clears the steep
/// part of the incumbent-quality cliff observed on realistic and
/// harder-than-realistic synthetic instances while bounding the worst case.
pub(crate) fn mip_time_limit() -> Duration {
    // See mip_rel_gap: unit tests need exact, unhurried solves on tiny
    // instances, not a production-scale wall-clock bound.
    #[cfg(test)]
    if let Some(v) = TEST_TIME_LIMIT_OVERRIDE.with(|c| c.get()) {
        return v;
    }
    #[cfg(test)]
    let default = Duration::from_secs(60);
    #[cfg(not(test))]
    let default = Duration::from_secs(5);

    get_duration_from_env("HQ_SCHEDULER_MIP_TIME_LIMIT_MS").unwrap_or(default)
}

fn get_f64_from_env(key: &str) -> Option<f64> {
    std::env::var(key).ok().and_then(|value| value.parse::<f64>().ok())
}

fn get_duration_from_env(key: &str) -> Option<Duration> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
}

#[cfg(test)]
thread_local! {
    static TEST_REL_GAP_OVERRIDE: std::cell::Cell<Option<f64>> = const { std::cell::Cell::new(None) };
    static TEST_TIME_LIMIT_OVERRIDE: std::cell::Cell<Option<Duration>> = const { std::cell::Cell::new(None) };
}

/// Runs `f` with a scheduler solver config override in effect, for tests
/// that need to exercise the production-tuned (or otherwise non-default)
/// solve_bounded() behavior. Thread-local, not a process-global env var: the
/// Rust test harness runs each #[test] to completion on its own OS thread,
/// so this cannot race with unrelated tests running concurrently on other
/// threads the way a process-global env var would.
#[cfg(test)]
pub(crate) fn with_test_solver_config<R>(rel_gap: f64, time_limit: Duration, f: impl FnOnce() -> R) -> R {
    TEST_REL_GAP_OVERRIDE.with(|c| c.set(Some(rel_gap)));
    TEST_TIME_LIMIT_OVERRIDE.with(|c| c.set(Some(time_limit)));
    let result = f();
    TEST_REL_GAP_OVERRIDE.with(|c| c.set(None));
    TEST_TIME_LIMIT_OVERRIDE.with(|c| c.set(None));
    result
}
