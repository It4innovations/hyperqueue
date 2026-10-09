use crate::internal::scheduler::gap::GapCache;
use crate::{Map, ResourceVariantId, TaskId, WorkerId};
use std::time::Duration;

/// How `prune_progressive` samples the tail of a batch's condition list, once the fixed prefix
/// (`SchedulerConfig::prune_fixed_prefix`) has been taken off the front.
///
/// Every shape keeps the *same number* of conditions for a given budget -- they differ only in
/// *which* ones survive. That is what makes the shapes comparable: `paper.tex` §6.4 claims the
/// quadratic schedule was chosen empirically over logarithmic/exponential alternatives, and this
/// enum is what lets the evaluation test that claim at a fixed condition count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PruneSchedule {
    /// Plain truncation: keep the head of the list, drop the rest. No sampling at all -- the
    /// baseline the sampled shapes have to beat.
    Head,
    /// Uniform spacing over the tail.
    Linear,
    /// `t^2` spacing: dense coverage near the high-priority head, increasingly sparse toward the
    /// tail. **This is what ships**, and the default.
    #[default]
    Quadratic,
    /// Geometric spacing -- even more head-dense than [`PruneSchedule::Quadratic`].
    Exponential,
    /// Deterministic pseudo-random sample of the tail: splitmix64 over the index, no RNG and no
    /// state, so the scheduler stays deterministic.
    ///
    /// A control arm for the evaluation, **not a production candidate**: it is the null hypothesis
    /// that a considered shape beats an arbitrary sample of the same size. It also scores the whole
    /// pool, O(pool) against the other shapes' O(size_limit) -- negligible beside a MILP solve, but
    /// another reason not to ship it.
    Random,
}

impl std::str::FromStr for PruneSchedule {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value.to_ascii_lowercase().as_str() {
            "head" => PruneSchedule::Head,
            "linear" => PruneSchedule::Linear,
            "quadratic" => PruneSchedule::Quadratic,
            "exponential" => PruneSchedule::Exponential,
            "random" => PruneSchedule::Random,
            _ => return Err(()),
        })
    }
}

impl std::fmt::Display for PruneSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PruneSchedule::Head => "head",
            PruneSchedule::Linear => "linear",
            PruneSchedule::Quadratic => "quadratic",
            PruneSchedule::Exponential => "exponential",
            PruneSchedule::Random => "random",
        })
    }
}

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
    /// Where the MILP backend should write its own solver log, if anywhere. The evaluation uses
    /// it to read the anytime behaviour of a solve: HiGHS logs every improved incumbent with a
    /// timestamp and the primal/dual bounds, which is what tells apart "still searching" from
    /// "already found, still proving". `None` in production; no log, no cost.
    pub mip_log_file: Option<std::path::PathBuf>,

    // ---- Ablation knobs, for the paper's evaluation (`benchmarks/paper/`). ----
    // All default to current behaviour, `prune_global_max` included -- it ships enabled,
    // see its doc below. Undocumented and testing-only: set via
    // `HQ_SCHED_*` environment variables (see `from_env`) or, in tests, via
    // `TestEnv::set_scheduler_config`. Deliberately not exposed as CLI flags.
    /// G: how many priority conditions survive across *all* batches. `usize::MAX` disables it.
    /// Unlike the other ablation knobs this one has a production default (64), and it is the only
    /// condition pruning there is -- the former per-batch cap (K) was removed.
    ///
    /// The total is the quantity that matters for the solve: `solver.rs`'s `get_bvar` memoizes one
    /// boolean variable per distinct `(blocker, size)`, so branch-and-bound cost follows the total
    /// condition count rather than any per-batch one. That is also why a per-batch cap could not
    /// bound it -- conditions spread thinly over many batches stay under any per-batch limit while
    /// their sum grows with the number of requests.
    ///
    /// Every batch is seeded with `prune_fixed_prefix` conditions **unconditionally** and the rest
    /// of the budget goes out round-robin. So the cap binds only above the floor
    /// `sum(min(cuts, prune_fixed_prefix))`: above it the surviving conditions total exactly G,
    /// and below it the guaranteed prefix wins and the total stays at the floor. The head
    /// conditions of each queue are what the exactness argument in `paper.tex` §6.4 rests on, so
    /// they are kept even when the budget cannot pay for them.
    pub prune_global_max: usize,
    /// F: how many leading priority conditions are always kept.
    pub prune_fixed_prefix: usize,
    /// The shape used to sample the conditions that survive *beyond* the fixed prefix. Purely an
    /// ablation knob: it never changes how many conditions survive, only which, so it cannot move
    /// the solve's size -- see [`PruneSchedule`].
    pub prune_schedule: PruneSchedule,
    /// Treat every gap as 0, i.e. never let a lower-priority task run alongside a
    /// blocked higher-priority one.
    pub disable_gaps: bool,
    /// Never emit reservation variables, so free capacity is never held back for a
    /// blocked request.
    pub disable_reservations: bool,
    /// Put every worker on the left-hand side of a priority condition, including
    /// those that cannot run the blocking request.
    pub disable_impossible_filter: bool,
    /// Enforce the *strict* priority rule: a blocker that cannot be served anywhere blocks all
    /// lower-priority work, instead of being treated as non-blocking.
    ///
    /// Off in production, where a blocker with no placement and no reservation variable is one
    /// that no worker can ever run -- Relaxation 1 (`paper.tex` §7.1) says such a blocker must
    /// not block anything, and the condition is correctly dropped. This flag exists so the
    /// evaluation can measure the strict baseline the relaxations are compared against; without
    /// it that baseline is unreachable, because relaxation 1 is structural rather than switchable.
    pub strict_rule: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            proactive_filling_reserve: 16,
            proactive_filling_max: 40,
            mip_time_limit: default_mip_time_limit(),
            mip_log_file: None,
            prune_global_max: 64,
            prune_fixed_prefix: 4,
            prune_schedule: PruneSchedule::Quadratic,
            disable_gaps: false,
            disable_reservations: false,
            disable_impossible_filter: false,
            strict_rule: false,
        }
    }
}

impl SchedulerConfig {
    /// Defaults overridden by `HQ_SCHED_*` environment variables. Used by the server at
    /// startup and by the evaluation's simulation driver, so both honour the same switches.
    /// An unparseable value is ignored with a warning rather than failing startup.
    pub fn from_env() -> Self {
        let mut config = SchedulerConfig::default();
        fn num<T: std::str::FromStr>(name: &str, target: &mut T) {
            match std::env::var(name) {
                Err(_) => {}
                Ok(value) => match value.parse() {
                    Ok(parsed) => *target = parsed,
                    Err(_) => log::warn!("Ignoring {name}: cannot parse {value:?}"),
                },
            }
        }
        fn flag(name: &str, target: &mut bool) {
            if let Ok(value) = std::env::var(name) {
                *target = !matches!(value.as_str(), "" | "0" | "false" | "no");
            }
        }
        num("HQ_SCHED_PRUNE_G", &mut config.prune_global_max);
        num("HQ_SCHED_PRUNE_F", &mut config.prune_fixed_prefix);
        num("HQ_SCHED_PRUNE_SCHEDULE", &mut config.prune_schedule);
        num("HQ_SCHED_PREFILL_MAX", &mut config.proactive_filling_max);
        num(
            "HQ_SCHED_PREFILL_RESERVE",
            &mut config.proactive_filling_reserve,
        );
        flag("HQ_SCHED_DISABLE_GAPS", &mut config.disable_gaps);
        flag(
            "HQ_SCHED_DISABLE_RESERVATIONS",
            &mut config.disable_reservations,
        );
        flag(
            "HQ_SCHED_DISABLE_IMPOSSIBLE_FILTER",
            &mut config.disable_impossible_filter,
        );
        flag("HQ_SCHED_STRICT_RULE", &mut config.strict_rule);
        config
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
