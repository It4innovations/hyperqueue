//! In-process scheduling simulation, for the paper's evaluation (`benchmarks/paper/README.md`).
//!
//! Runs the real pipeline -- `create_task_batches` -> `run_scheduling_solver` ->
//! `create_task_mapping` -- over synthetic states, with no processes, sockets or task
//! execution, and emits one CSV row per scheduling round from [`SchedulerRoundStats`].
//!
//! This lives inside the lib rather than in the `sched_sim` binary because the simulation
//! builders it reuses (`TestEnv`, `TaskBuilder`, `WorkerBuilder`) are `pub(crate)` or
//! `#[cfg(test)]`; a binary compiles as a separate crate and could not reach them. The public
//! surface is deliberately just [`SweepSpec`] and [`run_sweep`].

use crate::control::WorkerTypeQuery;
use crate::internal::scheduler::query::compute_new_worker_query_with_stats;
use crate::internal::scheduler::{
    PruneSchedule, SchedulerConfig, SchedulerRoundStats, SchedulingSolution, TaskBatch,
    create_task_batches, run_scheduling_solver,
};
use crate::internal::server::core::CoreSplit;
use crate::internal::server::task::TaskRuntimeState;
use crate::internal::tests::utils::env::TestEnv;
use crate::internal::tests::utils::task::TaskBuilder;
use crate::internal::tests::utils::worker::WorkerBuilder;
use crate::resources::ResourceRqId;
use crate::resources::{
    ResourceAmount, ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind,
};
use crate::{Map, Priority, Set, TaskId, WorkerId};
use std::time::{Duration, Instant};

/// One point of a parameter sweep.
#[derive(Debug, Clone)]
pub struct SweepSpec {
    /// Free-form label carried through to the CSV, so several sweeps can be concatenated.
    pub sweep: String,
    /// Workload shape. `uniform` is the parametric generator used by E1/E2; the others are the
    /// pathologies `paper.tex` §7 specifies exactly, so they are built literally rather than
    /// approximated with the generic knobs.
    pub scenario: String,
    pub n_workers: usize,
    /// Cpu count used when `worker_types` is empty.
    pub cpus_per_worker: u32,
    /// Distinct worker shapes, cycled across the cluster. Empty means a homogeneous cluster of
    /// `cpus_per_worker`. Heterogeneity is what makes the gap cache do any work (E6).
    pub worker_types: Vec<u32>,
    /// Number of distinct resource requests. Each gets its own queue and therefore its own
    /// batch, which is what drives the `|W| * |R|` term in the MILP size.
    pub n_request_types: u32,
    /// Number of distinct user-priority levels the tasks are spread over.
    pub priority_levels: u32,
    pub n_tasks: usize,
    /// Scheduling rounds to run per point.
    pub rounds: usize,
    /// Fraction of the tasks running after a round that are completed before the next one,
    /// so later rounds face a partially-occupied cluster instead of an empty one.
    pub drain_fraction: f64,
    /// Cap on a single MILP solve (`SchedulerConfig::mip_time_limit`). Production defaults to
    /// 5 s, which truncates the largest instances and reports `is_optimal = false`; raise it
    /// when the point of the sweep is the *true* solve cost rather than production behaviour.
    pub mip_time_limit_secs: f64,
    /// Directory for the MILP backend's own solver logs, one file per cell and round. Set by
    /// `--mip-log-dir`; the anytime analysis of a solve is read from these.
    pub mip_log_dir: Option<std::path::PathBuf>,
    /// G: total conditions surviving across all batches. `usize::MAX` disables it. Distinct from
    /// K whenever the cuts are spread thinly over many batches, which is the regime the
    /// priority-scaling sweep found in every slow cell.
    pub prune_global_max: usize,
    /// F: leading conditions always kept.
    pub prune_fixed_prefix: usize,
    /// The shape used to sample conditions beyond the fixed prefix. Count-equivalent across
    /// shapes by construction, so at a fixed `prune_global_max` this is the *only* variable --
    /// which is what makes it a controlled comparison, unlike sweeping the budget itself.
    pub prune_schedule: PruneSchedule,
    /// `proactive_filling_max`. Set to 0 for inversion measurement: prefilled tasks leave the
    /// regular queue, so they would otherwise read as "selected" in the bucket diff.
    pub prefill_max: u32,
    /// Cpus already busy on every worker when the run starts. All three §7 pathologies begin
    /// from a partially-occupied cluster, and the gap relaxation is only interesting there.
    pub occupied_cpus_per_worker: u32,
    /// Relaxations of the strict priority rule (`paper.tex` §7), for the E4 ablation.
    pub disable_impossible_filter: bool,
    pub disable_gaps: bool,
    pub disable_reservations: bool,
    /// Enforce the strict priority rule. Belongs with `disable_impossible_filter`: dropping a
    /// condition whose blocker has no variables *is* relaxation 1 applied globally, so the strict
    /// baseline needs both off.
    pub strict_rule: bool,
    /// Alternative resource variants per request ("cpus=4 or cpus=8"). 1 means a single-variant
    /// request.
    ///
    /// This is the only knob that makes the gap cache do any work: `GapCache::get_gap` takes the
    /// `trivial_request()` fast path whenever a request has exactly one variant, bypassing the
    /// memo without recording a hit or a miss. Heterogeneous *workers* do not help — the cache is
    /// keyed on (request, worker resources) but only reached for multi-variant requests.
    pub request_variants: u32,
    /// E8: `max_sn_workers` per query type. 0 disables the what-if query entirely, which is also
    /// the control -- a query for zero workers still runs the whole pipeline, so its cost is the
    /// floor that any reported query cost must be read against.
    pub query_workers: u32,
    /// Distinct worker types offered in one query call. Each contributes `query_workers` fake
    /// workers, so the model grows with the product.
    pub query_types: u32,
    /// Mark the query types partial, i.e. complete every undeclared resource with an unbounded
    /// `Sum`. This is what an unprobed PBS/SLURM queue looks like to the autoallocator, and it
    /// changes both the answer and the model.
    pub query_partial: bool,
    /// Evaluate the round's solution against the priority conditions that pruning removed, i.e.
    /// measure the scheduling error pruning actually causes. Off by default: it rebuilds the
    /// batches unpruned and walks every cut, which must not silently tax the timing sweeps.
    pub check_pruning: bool,
    /// Also *solve* the unpruned model on the same state, to get the throughput the priority rule
    /// actually permits (`PruneError::ref_assigned`).
    ///
    /// Separate from `check_pruning` because it costs a whole extra MILP solve per round, up to
    /// `mip_time_limit`, where `check_pruning` costs only a cut walk. Folding it in would make
    /// every existing timing sweep incomparable to its predecessors.
    pub reference_solve: bool,
}

impl Default for SweepSpec {
    fn default() -> Self {
        SweepSpec {
            sweep: "default".to_string(),
            scenario: "uniform".to_string(),
            n_workers: 16,
            cpus_per_worker: 16,
            worker_types: Vec::new(),
            n_request_types: 1,
            priority_levels: 1,
            n_tasks: 1000,
            rounds: 3,
            drain_fraction: 0.5,
            mip_time_limit_secs: 5.0,
            mip_log_dir: None,
            prune_global_max: usize::MAX,
            prune_fixed_prefix: 4,
            prune_schedule: PruneSchedule::Quadratic,
            prefill_max: 40,
            request_variants: 1,
            occupied_cpus_per_worker: 0,
            disable_impossible_filter: false,
            disable_gaps: false,
            disable_reservations: false,
            strict_rule: false,
            query_workers: 0,
            query_types: 1,
            query_partial: false,
            check_pruning: false,
            reference_solve: false,
        }
    }
}

/// A single round's measurements together with the parameters that produced them.
#[derive(Debug, Clone)]
pub struct SweepRow {
    pub spec: SweepSpec,
    pub round: usize,
    pub stats: SchedulerRoundStats,
    /// Wall time spent building the state, reported once (round 0) so the cost of large |T|
    /// is visible and never confused with scheduling time.
    pub build_seconds: f64,
    pub inversions: Inversions,
    /// E8: the what-if query run on the same state as this round, when `query_workers > 0`.
    pub query: Option<QueryMeasurement>,
    /// The scheduling error pruning caused this round, when `check_pruning` is set.
    pub prune_error: Option<PruneError>,
}

/// How far the dispatched solution departs from the priority conditions pruning removed.
///
/// Pruning only ever *deletes* conditions, so the pruned model's constraints are a strict subset
/// of the full model's: the solution it produced is feasible in the full model exactly when it
/// satisfies the dropped conditions. That makes this a direct measurement needing no capacity
/// heuristic -- unlike `inverted`, whose post-round placeability check excuses precisely the harm
/// pruning causes (low-priority work takes the capacity, so the higher-priority request is no
/// longer placeable, so the violation is not counted).
#[derive(Debug, Clone, Default)]
pub struct PruneError {
    /// Conditions in the unpruned cut set for this state.
    pub full_cuts: u32,
    /// Conditions the solver actually saw.
    pub kept_cuts: u32,
    /// Conditions removed by pruning, i.e. the ones the solver never saw.
    pub dropped_cuts: u32,
    /// How many of those the dispatched solution violates.
    pub dropped_violated: u32,
    /// Total tasks placed beyond what the dropped conditions allowed, summed over violations.
    /// Double-counts nested conditions; see `misplaced_tasks` for a count that does not.
    pub total_excess: u32,
    /// Tasks placed in violation of a dropped condition, counted once each (see
    /// `CutEvaluation::misplaced_tasks`).
    ///
    /// This is the one priority-error number with a denominator: divided by `n_tasks_assigned` it
    /// gives the fraction of dispatched work that jumped the queue, which -- unlike
    /// `dropped_violated` -- does not grow simply because an arm dispatched more.
    pub misplaced_tasks: u32,
    /// Largest single overshoot, in tasks.
    pub max_excess: u32,
    /// Violations among the cuts that were *kept*. These were constraints of the very model that
    /// produced this solution, so a correct evaluator must report 0. Anything else means this
    /// evaluator has diverged from `solver.rs` and every other field here is void.
    pub kept_violated: u32,
    /// Multi-node cuts skipped: they constrain worker *groups* rather than workers, a different
    /// constraint form. Reported rather than folded in silently.
    pub skipped_multinode: u32,

    // ---- The unpruned reference, when `SweepSpec::reference_solve` is set. ----
    /// Tasks the *unpruned* model dispatches on this state: the throughput the priority rule
    /// permits. Without it neither `dropped_violated` nor the round's own `n_tasks_assigned` can
    /// be read as quality -- both grow with how much work was dispatched, so an arm that jumps the
    /// queue and an arm that simply schedules well are indistinguishable. Work dispatched *beyond*
    /// this is precisely priority-rule violation.
    pub ref_assigned: Option<u32>,
    /// `false` means the reference hit `mip_time_limit` and is a truncated incumbent, **not**
    /// ground truth. On heavily-interleaved states this is the common case -- it is exactly why
    /// pruning exists -- and no relative-quality claim can be made on such a row.
    pub ref_is_optimal: Option<bool>,
    /// Constraints in the unpruned model, i.e. the model size pruning avoids.
    pub ref_n_constraints: Option<u32>,
    /// The reference solution evaluated against its *own* full cut set.
    ///
    /// A self-check, not a result: an optimal solution to the unpruned model satisfies every
    /// condition in it, so `ref_is_optimal && ref_violated > 0` means this evaluator has diverged
    /// from `solver.rs` and every quality number in the row is void. The analogue of
    /// `kept_violated`, but exercising `evaluate_cuts` against a *different* solution.
    pub ref_violated: Option<u32>,
    /// The MILP objective of the arm's own solution, and of the reference's.
    ///
    /// This is the currency the solver actually optimises — a placement's weight is
    /// `resource share x compaction bias x request weight`, not one per task — so it, not
    /// `ref_assigned`, is what "how good is this solution" means. Two consequences follow from
    /// pruning being a *relaxation* (it only deletes constraints, so the feasible set grows), and
    /// both are checked in the tests below and by the analysis scripts:
    ///
    /// * `arm_objective >= ref_objective` whenever both solves are optimal — a relaxed model can
    ///   never score worse;
    /// * if additionally `dropped_violated == 0`, the arm's solution is feasible in the full model,
    ///   so the two are **equal** and the arm found an *alternative optimum* — a different argmax,
    ///   not a better schedule.
    pub arm_objective: Option<f64>,
    pub ref_objective: Option<f64>,
}

/// One what-if query, measured on the same state as the scheduling round beside it. The pairing
/// is the point: the absolute microseconds depend on the host, the ratio to a regular round does
/// not, and the round is what the server would otherwise be doing with that time.
#[derive(Debug, Clone)]
pub struct QueryMeasurement {
    /// Wall time inside `compute_new_worker_query`. `Core` is borrowed exclusively for all of it,
    /// so this is server-thread blocking time, not background work.
    pub seconds: f64,
    pub n_variables: u32,
    pub n_constraints: u32,
    /// `false` means the solve hit `mip_time_limit` and the answer is truncated. A fast row that
    /// is not optimal is not a cheap query.
    pub is_optimal: bool,
    pub n_fake_workers: usize,
    /// Workers the query said would be useful, summed over the query types.
    pub reported: u32,
}

/// Violations of the strict priority rule observed in one round (`paper.tex`, def. 423):
/// *if a schedulable task is selected, every schedulable task of higher priority is selected too*.
///
/// Measured per `(request, priority)` bucket rather than per task, because task counts are
/// dominated by how many tasks happen to share a level. "Schedulable" is the solver's own test --
/// some worker passes `have_immediate_resources_for_rq` at round start, which is exactly the
/// condition under which a placement variable is created.
#[derive(Debug, Clone, Default)]
pub struct Inversions {
    /// Buckets that were schedulable at round start; the denominator for `inverted`.
    pub schedulable_levels: u32,
    /// Buckets left unselected although a strictly lower priority was selected **and** whose
    /// request still had free resources when the round finished, so it demonstrably could have
    /// been placed instead. This is the metric that tests the rule.
    ///
    /// The post-round check is not optional. When a priority level over-subscribes the cluster
    /// some of its tasks must remain unselected, and the leftover capacity is often a shape only
    /// a smaller request can use -- a 3-cpu hole takes a 1-cpu task but not a 16-cpu one.
    /// Counting those as violations would make the rule unsatisfiable by construction; measured
    /// with pruning disabled, that is exactly what `inverted_strict` reports and this does not.
    pub inverted: u32,
    /// The same count without the post-round capacity check. Kept because the gap between the
    /// two is the "leftover holes too small for the waiting task" effect, not scheduler error.
    pub inverted_strict: u32,
    /// Rank of the highest-priority violated level in this round's descending priority order,
    /// or `None` if there were no violations. The paper claims the fixed prefix `F` keeps the
    /// head exact, i.e. this is always `>= F`.
    pub min_inverted_rank: Option<u32>,
}

pub fn csv_header() -> String {
    [
        "sweep",
        "n_workers",
        "cpus_per_worker",
        "n_request_types",
        "priority_levels",
        "n_tasks",
        "worker_types",
        "mip_time_limit_s",
        "prune_g",
        "prune_f",
        "prune_schedule",
        "prefill_max",
        "request_variants",
        "occupied_cpus",
        "relaxations",
        "scenario",
        "round",
        "build_seconds",
        "t_batches_us",
        "t_solve_us",
        "t_mapping_us",
        "t_send_us",
        "t_total_us",
        "n_variables",
        "n_constraints",
        "n_batches",
        "n_cuts_before_prune",
        "n_cuts_after_prune",
        "n_tasks_considered",
        "n_tasks_assigned",
        "cpus_assigned",
        "cpus_free",
        "n_workers_used",
        "objective",
        "gap_cache_hits",
        "gap_cache_misses",
        "is_optimal",
        "schedulable_levels",
        "inverted_levels",
        "inverted_strict",
        "min_inverted_rank",
        "query_workers",
        "query_types",
        "query_partial",
        "t_query_us",
        "query_n_variables",
        "query_n_constraints",
        "query_is_optimal",
        "query_fake_workers",
        "query_reported",
        "full_cuts",
        "kept_cuts",
        "dropped_cuts",
        "dropped_violated",
        "prune_total_excess",
        "prune_misplaced_tasks",
        "prune_max_excess",
        "kept_violated",
        "skipped_multinode",
        "ref_assigned",
        "ref_is_optimal",
        "ref_n_constraints",
        "ref_violated",
        "arm_objective",
        "ref_objective",
    ]
    .join(",")
}

impl SweepRow {
    /// One CSV row. Built as a list of fields rather than a positional format string: the column
    /// set has grown repeatedly, and a miscounted `{}` silently applies a numeric precision spec
    /// to a text field (truncating it) instead of failing to compile.
    pub fn to_csv(&self) -> String {
        let s = &self.stats;
        let us = |d: std::time::Duration| format!("{:.1}", d.as_secs_f64() * 1e6);
        let spec = &self.spec;
        let worker_types = if spec.worker_types.is_empty() {
            spec.cpus_per_worker.to_string()
        } else {
            spec.worker_types
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(";")
        };
        // Which relaxations are active, as a short stable label for the ablation.
        let relaxations = match (
            spec.strict_rule,
            spec.disable_impossible_filter,
            spec.disable_gaps,
            spec.disable_reservations,
        ) {
            (true, true, true, true) => "strict",
            (true, true, false, true) => "strict+gaps",
            (false, false, true, true) => "impossible",
            (false, false, false, true) => "impossible+gaps",
            (false, false, false, false) => "all",
            _ => "custom",
        };
        let fields: Vec<String> = vec![
            spec.sweep.clone(),
            spec.n_workers.to_string(),
            spec.cpus_per_worker.to_string(),
            spec.n_request_types.to_string(),
            spec.priority_levels.to_string(),
            spec.n_tasks.to_string(),
            worker_types,
            spec.mip_time_limit_secs.to_string(),
            spec.prune_global_max.to_string(),
            spec.prune_fixed_prefix.to_string(),
            spec.prune_schedule.to_string(),
            spec.prefill_max.to_string(),
            spec.request_variants.to_string(),
            spec.occupied_cpus_per_worker.to_string(),
            relaxations.to_string(),
            spec.scenario.clone(),
            self.round.to_string(),
            format!("{:.3}", self.build_seconds),
            us(s.t_batches),
            us(s.t_solve),
            us(s.t_mapping),
            us(s.t_send),
            us(s.total_time()),
            s.n_variables.to_string(),
            s.n_constraints.to_string(),
            s.n_batches.to_string(),
            s.n_cuts_before_prune.to_string(),
            s.n_cuts_after_prune.to_string(),
            s.n_tasks_considered.to_string(),
            s.n_tasks_assigned.to_string(),
            format!("{:.4}", s.cpus_assigned),
            format!("{:.4}", s.cpus_free),
            s.n_workers_used.to_string(),
            s.objective.map(|o| format!("{o:.6}")).unwrap_or_default(),
            s.gap_cache_hits.to_string(),
            s.gap_cache_misses.to_string(),
            s.is_optimal.to_string(),
            self.inversions.schedulable_levels.to_string(),
            self.inversions.inverted.to_string(),
            self.inversions.inverted_strict.to_string(),
            self.inversions
                .min_inverted_rank
                .map(|r| r.to_string())
                .unwrap_or_default(),
            spec.query_workers.to_string(),
            spec.query_types.to_string(),
            spec.query_partial.to_string(),
            // Empty rather than 0 when no query ran, so a disabled query is never averaged in as
            // a free one.
            self.query
                .as_ref()
                .map(|q| format!("{:.1}", q.seconds * 1e6))
                .unwrap_or_default(),
            self.query
                .as_ref()
                .map(|q| q.n_variables.to_string())
                .unwrap_or_default(),
            self.query
                .as_ref()
                .map(|q| q.n_constraints.to_string())
                .unwrap_or_default(),
            self.query
                .as_ref()
                .map(|q| q.is_optimal.to_string())
                .unwrap_or_default(),
            self.query
                .as_ref()
                .map(|q| q.n_fake_workers.to_string())
                .unwrap_or_default(),
            self.query
                .as_ref()
                .map(|q| q.reported.to_string())
                .unwrap_or_default(),
            // Empty when the check did not run, so "not measured" never reads as "zero errors".
            self.prune_error
                .as_ref()
                .map(|e| e.full_cuts.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .map(|e| e.kept_cuts.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .map(|e| e.dropped_cuts.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .map(|e| e.dropped_violated.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .map(|e| e.total_excess.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .map(|e| e.misplaced_tasks.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .map(|e| e.max_excess.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .map(|e| e.kept_violated.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .map(|e| e.skipped_multinode.to_string())
                .unwrap_or_default(),
            // Doubly optional: empty both when the check did not run and when it ran without
            // `--reference-solve`, so an absent reference never reads as a zero-throughput one.
            self.prune_error
                .as_ref()
                .and_then(|e| e.ref_assigned)
                .map(|v| v.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .and_then(|e| e.ref_is_optimal)
                .map(|v| v.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .and_then(|e| e.ref_n_constraints)
                .map(|v| v.to_string())
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .and_then(|e| e.ref_violated)
                .map(|v| v.to_string())
                .unwrap_or_default(),
            // Enough digits that the equality theorem above is checkable from the CSV rather than
            // only in-process: these are sums of small fractions.
            self.prune_error
                .as_ref()
                .and_then(|e| e.arm_objective)
                .map(|v| format!("{v:.12}"))
                .unwrap_or_default(),
            self.prune_error
                .as_ref()
                .and_then(|e| e.ref_objective)
                .map(|v| format!("{v:.12}"))
                .unwrap_or_default(),
        ];
        debug_assert_eq!(fields.len(), csv_header().split(',').count());
        fields.join(",")
    }
}

/// Cpu count for request `index`, chosen so that `n_request_types` distinct requests really do
/// produce `n_request_types` distinct queues: consecutive integers, never exceeding a worker.
///
/// This caps `n_request_types` at `cpus_per_worker` — asking for more distinct requests than a
/// worker has cpus would silently collide and understate the MILP size, so callers sweeping the
/// request dimension must scale `cpus_per_worker` with it.
fn request_cpus(index: u32, cpus_per_worker: u32) -> u32 {
    let width = cpus_per_worker.max(1);
    1 + (index % width)
}

/// Build the tasks (and any pre-existing occupancy) for a sweep point.
fn build_workload(rt: &mut TestEnv, spec: &SweepSpec, worker_ids: &[WorkerId]) {
    if spec.occupied_cpus_per_worker > 0 {
        // Held by running tasks, so the scheduler sees a partially-occupied cluster from round 0.
        for &worker_id in worker_ids {
            rt.new_task_running(
                &TaskBuilder::new().cpus(spec.occupied_cpus_per_worker),
                worker_id,
            );
        }
    }

    match spec.scenario.as_str() {
        // `paper.tex` §7.3: "ten 8-CPU workers, each with 4 CPUs occupied; a new submit brings one
        // 6-CPU task at priority 2 and a hundred 1-CPU tasks at priority 1". No worker can host
        // the 6-CPU task now, so strict idles the lot; reservations should hold one worker.
        "reservation" => {
            rt.new_task(&TaskBuilder::new().cpus(6).user_priority(2));
            for _ in 0..spec.n_tasks {
                rt.new_task(&TaskBuilder::new().cpus(1).user_priority(1));
            }
        }
        // `paper.tex` §7.1: a 4-CPU task at priority 2 and 2-CPU tasks at priority 1, with workers
        // too small to ever run the blocker. The strict rule blocks the small tasks cluster-wide.
        "impossible" => {
            rt.new_task(&TaskBuilder::new().cpus(4).user_priority(2));
            for _ in 0..spec.n_tasks {
                rt.new_task(&TaskBuilder::new().cpus(2).user_priority(1));
            }
        }
        // The sampling-schedule comparison of the supplementary material: where in the priority
        // range the requests interleave. `band-all` puts every request at every level; the others
        // interleave all requests only in a band of a tenth of the levels, at the head, middle or
        // tail, and give every other level a single request, alternating.
        band if band.starts_with("band-") => build_band_workload(rt, spec, band),
        _ => {
            // Requests cycle *within* a priority level, so every (request, priority) pair occurs
            // and priorities of different requests interleave -- the worst case named in §6.4.
            let n_requests = spec.n_request_types.max(1);
            for i in 0..spec.n_tasks {
                let cpus = request_cpus(i as u32 % n_requests, spec.cpus_per_worker);
                let priority = ((i as u32 / n_requests) % spec.priority_levels.max(1)) as i32;
                let mut builder = TaskBuilder::new().cpus(cpus).user_priority(priority);
                for v in 1..spec.request_variants.max(1) {
                    let alt = request_cpus(i as u32 % n_requests + v, spec.cpus_per_worker);
                    builder = builder.next_variant().cpus(alt);
                }
                rt.new_task(&builder);
            }
        }
    }
}

/// Tasks for the `band-*` scenarios; see `build_workload`.
///
/// Every (level, request) pair that the scenario puts in the queue is one entry, and the tasks are
/// spread evenly over the entries, so the shape is the same at every queue length. The highest
/// priority is `priority_levels - 1`, so the head band holds the top levels.
fn build_band_workload(rt: &mut TestEnv, spec: &SweepSpec, scenario: &str) {
    let levels = spec.priority_levels.max(1);
    let n_requests = spec.n_request_types.max(1);
    let band = (levels / 10).max(1);
    let band_start = match scenario {
        "band-all" => 0,
        "band-head" => levels - band,
        "band-middle" => (levels - band) / 2,
        "band-tail" => 0,
        other => panic!("Unknown band scenario {other:?}"),
    };
    let band_end = if scenario == "band-all" {
        levels
    } else {
        band_start + band
    };
    let mut entries: Vec<(u32, u32)> = Vec::new();
    for level in 0..levels {
        if (band_start..band_end).contains(&level) {
            entries.extend((0..n_requests).map(|request| (level, request)));
        } else {
            entries.push((level, level % n_requests));
        }
    }
    for i in 0..spec.n_tasks {
        let (level, request) = entries[i * entries.len() / spec.n_tasks];
        let cpus = request_cpus(request, spec.cpus_per_worker);
        rt.new_task(&TaskBuilder::new().cpus(cpus).user_priority(level as i32));
    }
}

/// Waiting tasks grouped by `(resource request, priority)`, plus whether each request had any
/// worker with immediate free resources. Both are read straight off the live queues, so a round's
/// selections can be recovered by diffing two snapshots.
fn snapshot(rt: &mut TestEnv) -> (Map<(ResourceRqId, Priority), u32>, Set<ResourceRqId>) {
    let mut waiting: Map<(ResourceRqId, Priority), u32> = Map::new();
    let mut schedulable: Set<ResourceRqId> = Set::new();
    let now = std::time::Instant::now();

    let CoreSplit {
        worker_map,
        task_queues,
        request_map,
        ..
    } = rt.core().split();
    for queue in task_queues.iter() {
        let rq_id = queue.resource_rq_id;
        for (priority, size) in queue.iter_priority_sizes() {
            *waiting.entry((rq_id, priority)).or_insert(0) += size;
        }
        // The solver's own schedulability test: a placement variable exists exactly when some
        // worker can host the request right now.
        let rqv = request_map.get(rq_id);
        let runnable = worker_map.get_workers().any(|w| {
            rqv.requests().iter().any(|rq| {
                w.has_time_to_run(rq.min_time(), now) && w.have_immediate_resources_for_rq(rq)
            })
        });
        if runnable {
            schedulable.insert(rq_id);
        }
    }
    (waiting, schedulable)
}

/// Compare snapshots taken either side of a round and count violations of the strict priority rule.
fn measure_inversions(
    before: &Map<(ResourceRqId, Priority), u32>,
    schedulable: &Set<ResourceRqId>,
    after: &Map<(ResourceRqId, Priority), u32>,
    schedulable_after: &Set<ResourceRqId>,
) -> Inversions {
    // Lowest priority that actually got something scheduled this round.
    let mut lowest_selected: Option<Priority> = None;
    for (key, before_count) in before {
        let after_count = after.get(key).copied().unwrap_or(0);
        if after_count < *before_count {
            lowest_selected = Some(match lowest_selected {
                Some(p) if p <= key.1 => p,
                _ => key.1,
            });
        }
    }
    let Some(lowest_selected) = lowest_selected else {
        return Inversions::default(); // nothing scheduled: nothing to violate
    };

    // Descending priority order over schedulable buckets, so a violation can be given a rank.
    let mut levels: Vec<(Priority, ResourceRqId)> = before
        .keys()
        .filter(|(rq_id, _)| schedulable.contains(rq_id))
        .map(|(rq_id, priority)| (*priority, *rq_id))
        .collect();
    levels.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));

    let mut inversions = Inversions {
        schedulable_levels: levels.len() as u32,
        ..Default::default()
    };
    for (rank, (priority, rq_id)) in levels.into_iter().enumerate() {
        if priority <= lowest_selected {
            continue; // not higher-priority than something that ran
        }
        let remaining = after.get(&(rq_id, priority)).copied().unwrap_or(0);
        if remaining == 0 {
            continue;
        }
        inversions.inverted_strict += 1;
        // Only a real violation if the request could still have been placed after the round.
        if schedulable_after.contains(&rq_id) {
            inversions.inverted += 1;
            let rank = rank as u32;
            inversions.min_inverted_rank =
                Some(inversions.min_inverted_rank.map_or(rank, |r| r.min(rank)));
        }
    }
    inversions
}

/// Build a synthetic state and run `spec.rounds` scheduling rounds against it.
pub fn run_sweep(spec: &SweepSpec) -> Vec<SweepRow> {
    assert!(
        spec.n_request_types <= spec.cpus_per_worker.max(1),
        "n_request_types ({}) exceeds cpus_per_worker ({}); distinct requests would collide \
         and understate the model size",
        spec.n_request_types,
        spec.cpus_per_worker
    );
    let build_start = Instant::now();
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(scheduler_config(spec));
    let worker_ids: Vec<WorkerId> = (0..spec.n_workers)
        .map(|i| {
            let cpus = if spec.worker_types.is_empty() {
                spec.cpus_per_worker
            } else {
                spec.worker_types[i % spec.worker_types.len()]
            };
            rt.new_worker(&WorkerBuilder::new(cpus))
        })
        .collect();

    build_workload(&mut rt, spec, &worker_ids);
    let build_seconds = build_start.elapsed().as_secs_f64();

    let mut rows = Vec::with_capacity(spec.rounds);
    for round in 0..spec.rounds {
        // One solver log per cell and round, so a truncated solve can be read back as a curve of
        // incumbents over time rather than a single end value.
        if let Some(dir) = &spec.mip_log_dir {
            let mut config = scheduler_config(spec);
            config.mip_log_file = Some(dir.join(format!("{}-r{round}.log", spec.cell_tag())));
            rt.set_scheduler_config(config);
        }
        let (before, schedulable) = snapshot(&mut rt);
        // Before the round is applied: this judges the state the round actually solves. Measuring
        // afterwards silently reports zeroes, because the dispatched tasks have left the queue and
        // the rebuilt batches no longer resemble the ones the solver saw.
        let prune_error = measure_prune_error(&mut rt, spec);
        let (_comm, stats) = rt.schedule_with_stats();
        let (after, schedulable_after) = snapshot(&mut rt);
        // Measured after the round, on the state the round left behind -- which is when the
        // autoallocator actually asks. Querying the pre-round state would ask "do I need workers"
        // about tasks the scheduler was about to place anyway.
        let query = measure_query(&mut rt, spec);
        rows.push(SweepRow {
            spec: spec.clone(),
            round,
            stats,
            build_seconds: if round == 0 { build_seconds } else { 0.0 },
            inversions: measure_inversions(&before, &schedulable, &after, &schedulable_after),
            query,
            prune_error,
        });
        if round + 1 < spec.rounds {
            drain(&mut rt, &worker_ids, spec.drain_fraction);
        }
    }
    rows
}

/// Evaluate one cut set against a dispatched solution, mirroring the constraint forms in
/// `solver.rs`'s cut loop.
///
/// Mirrors, specifically: a condition is discharged when the blocker got its `size` tasks, counting
/// tasks *placed or held* -- a reservation holds a whole worker for one pending blocker task and
/// counts toward the blocker in the model, and reading `sn_counts` alone (placements only) is what
/// used to make this mirror invent violations of conditions the solver had discharged. On workers
/// with `gap > 0` the bound is per worker (`<= cut.size + gap`); on workers with `gap == 0` the
/// solver aggregates across workers, so this does too -- reading that case per worker would
/// under-report.
///
/// The invariant this protects is `kept_violated == 0`: kept cuts are constraints of the model that
/// produced the solution, so anything they report is evaluator error. The remaining known
/// divergences all under-report and none can invent a violation: reserved workers take the
/// unconditional form in the solver, which is *stricter* than the conditional one evaluated here,
/// and a discharged condition is skipped entirely even though the solver still binds its reserved
/// workers unconditionally -- a set reservation consumes the worker's capacity, so placements
/// there are zero anyway.
/// What one pass of `evaluate_cuts` found. A named struct rather than a tuple because callers read
/// individual fields (`kept_violated`, the reference's own violation count) and a positional `.0`
/// at four call sites is easy to get silently wrong.
struct CutEvaluation {
    /// Conditions the solution breaks.
    violated: u32,
    /// Each violated condition's worst overshoot, summed. **Overlapping conditions are counted
    /// repeatedly**: cuts within a batch are generated at increasing `cut.size` over the same
    /// request, so their task sets are nested. Read it as severity, not as a count of tasks --
    /// `misplaced_tasks` is the count.
    total_excess: u32,
    /// Largest single overshoot, in tasks.
    max_excess: u32,
    /// Tasks placed in violation of a dropped condition, **counted once each**.
    ///
    /// Within a batch the cuts are nested -- overshoot is the cluster-wide work beyond the gaps
    /// minus `cut.size`, and `size` grows down the priority order -- so the *tightest* cut's task
    /// set contains every other's,
    /// and its overshoot is their maximum. Taking that maximum per batch and summing across
    /// batches therefore counts each placement exactly once: batches are distinct resource
    /// requests, and `placed_on` is keyed by `(batch_rq, worker)`, so no two batches can claim the
    /// same placement. Hence `misplaced_tasks <= n_tasks_assigned`, which is what makes it
    /// meaningful as a *fraction* of dispatched work -- unlike `dropped_violated`, it does not
    /// grow merely because an arm dispatched more.
    misplaced_tasks: u32,
    /// Multi-node cuts skipped: they constrain worker groups, a different constraint form.
    skipped_multinode: u32,
}

fn evaluate_cuts(
    rt: &mut TestEnv,
    batches: &[TaskBatch],
    solution: &SchedulingSolution,
    now: Instant,
) -> CutEvaluation {
    let core = rt.core();
    let CoreSplit {
        task_map,
        worker_map,
        request_map,
        scheduler_state,
        ..
    } = core.split();

    // Tasks placed per request, and per (request, worker), from the dispatched solution.
    let mut placed_total: Map<ResourceRqId, u32> = Map::new();
    let mut placed_on: Map<(ResourceRqId, WorkerId), u32> = Map::new();
    for ((rq_id, _v_id), per_worker) in &solution.sn_counts {
        for (w_id, count) in per_worker {
            *placed_total.entry(*rq_id).or_default() += *count;
            *placed_on.entry((*rq_id, *w_id)).or_default() += *count;
        }
    }

    let (mut violated, mut total_excess, mut max_excess, mut skipped) = (0u32, 0u32, 0u32, 0u32);
    let mut misplaced_tasks = 0u32;
    for batch in batches {
        let batch_rqv = request_map.get(batch.resource_rq_id);
        if batch_rqv.is_multi_node() {
            skipped += batch.cuts.len() as u32;
            continue;
        }
        // The tightest violated condition in this batch subsumes the rest (see
        // `CutEvaluation::misplaced_tasks`), so the batch contributes its maximum, not its sum.
        let mut batch_worst = 0u32;
        for cut in &batch.cuts {
            let mut worst = 0u32;
            for (blocker_rq_id, blocking_size) in &cut.blockers {
                let blocker_rqv = request_map.get(*blocker_rq_id);
                if blocker_rqv.is_multi_node() {
                    continue;
                }
                // Discharged: the blocker got what the condition asked for -- placed *or* held.
                // A reservation consumes a whole worker for one pending blocker task and counts
                // toward the blocker's total in the model (`get_bvar` sums `tasks_count_vars`,
                // which holds placements and reservations alike), so a mirror reading `sn_counts`
                // alone treats a condition the solver discharged as still binding and reports a
                // violation that was never in the model.
                if let Some(s) = blocking_size
                    && placed_total.get(blocker_rq_id).copied().unwrap_or(0)
                        + solution
                            .reserved_counts
                            .get(blocker_rq_id)
                            .copied()
                            .unwrap_or(0)
                        >= *s
                {
                    continue;
                }
                // The before-count is one allowance for the whole cluster: a worker's gap tasks
                // are exempt, and everything beyond a worker's gap counts against it once. This
                // mirrors `solver.rs`'s condition, where the gap-usage variables are subtracted
                // from a single sum over the capable workers -- not a per-worker copy of
                // `cut.size`, which is what this check used before the encoding was fixed.
                let mut beyond_gap = 0u32;
                for w in worker_map.get_workers() {
                    let Some(sn) = w.sn_assignment() else {
                        continue;
                    };
                    if !w.is_capable_to_run_rqv(blocker_rqv, now) {
                        continue;
                    }
                    let gap = scheduler_state.gap_cache.get_gap(
                        *blocker_rq_id,
                        batch.resource_rq_id,
                        &w.resources,
                        sn.assigned_tasks.iter().map(|task_id| {
                            let t = task_map.get_task(*task_id);
                            (
                                t.resource_rq_id,
                                t.assigned_placement(&scheduler_state.redirects).unwrap().1,
                            )
                        }),
                        request_map,
                    );
                    let placed = placed_on
                        .get(&(batch.resource_rq_id, w.id))
                        .copied()
                        .unwrap_or(0);
                    beyond_gap += placed.saturating_sub(gap);
                }
                worst = worst.max(beyond_gap.saturating_sub(cut.size));
            }
            if worst > 0 {
                violated += 1;
                total_excess += worst;
                max_excess = max_excess.max(worst);
                batch_worst = batch_worst.max(worst);
            }
        }
        misplaced_tasks += batch_worst;
    }
    CutEvaluation {
        violated,
        total_excess,
        max_excess,
        misplaced_tasks,
        skipped_multinode: skipped,
    }
}

/// The scheduler configuration a spec asks for. Factored out because `measure_prune_error` has to
/// restore it after temporarily disabling pruning, and a second inline copy would drift.
impl SweepSpec {
    /// Slug identifying this cell, for per-cell output files.
    fn cell_tag(&self) -> String {
        format!(
            "{}-w{}-c{}-r{}-p{}-t{}-g{}",
            self.sweep,
            self.n_workers,
            self.cpus_per_worker,
            self.n_request_types,
            self.priority_levels,
            self.n_tasks,
            self.prune_global_max
        )
    }
}

fn scheduler_config(spec: &SweepSpec) -> SchedulerConfig {
    SchedulerConfig {
        mip_time_limit: Duration::from_secs_f64(spec.mip_time_limit_secs),
        prune_global_max: spec.prune_global_max,
        prune_fixed_prefix: spec.prune_fixed_prefix,
        prune_schedule: spec.prune_schedule,
        proactive_filling_max: spec.prefill_max,
        disable_impossible_filter: spec.disable_impossible_filter,
        disable_gaps: spec.disable_gaps,
        disable_reservations: spec.disable_reservations,
        strict_rule: spec.strict_rule,
        ..Default::default()
    }
}

/// Measure the scheduling error pruning caused, by re-deriving the *unpruned* cut set for the
/// current state and evaluating the round's own solution against it.
///
/// The two batch builds are cheap -- E1 measured partitioning at 0.01-0.05 ms even at 10^7 tasks,
/// and `create_task_batches` is idempotent (it reads the queues, never pops) -- so the cost here is
/// the cut walk, not a second solve.
///
/// Note this re-solves the *pruned* batches to obtain the solution being judged, rather than
/// reusing the round's dispatched one: `schedule_with_stats` applies its result and does not hand
/// the solution back. Same state, same config, so it is the same model.
fn measure_prune_error(rt: &mut TestEnv, spec: &SweepSpec) -> Option<PruneError> {
    if !spec.check_pruning {
        return None;
    }
    let now = rt.now();
    let pruned = create_task_batches(rt.core(), now, None);
    let solution = run_scheduling_solver(rt.core(), now, &pruned, None);

    // Same state, pruning effectively disabled: the conditions the solver never saw.
    let mut unpruned_config = scheduler_config(spec);
    // Both caps, not just K. Leaving G on would globally prune the reference set too, so every
    // dropped condition it removed would be counted as never generated and the error would read
    // as zero -- and `kept_violated` cannot catch that, since it only checks the cuts that
    // survived.
    unpruned_config.prune_global_max = usize::MAX;
    // `prune_schedule` deliberately needs no reset here: `apply_global_cut_budget` returns early
    // when the total fits the budget, so with G disabled no shape is ever applied to the reference
    // set. Resetting it too would be harmless but would imply the shape mattered here.

    rt.set_scheduler_config(unpruned_config);
    let full = create_task_batches(rt.core(), now, None);
    // Solved here, while the unpruned config is still installed, so the reference gets the same
    // relaxation flags and the same `mip_time_limit` as the arm it is the reference for.
    let reference = spec
        .reference_solve
        .then(|| run_scheduling_solver(rt.core(), now, &full, None));
    rt.set_scheduler_config(scheduler_config(spec));

    let kept_violated = evaluate_cuts(rt, &pruned, &solution, now).violated;
    let full_eval = evaluate_cuts(rt, &full, &solution, now);

    // Judged against the full cut set, exactly as the arm's solution is.
    let ref_violated = reference
        .as_ref()
        .map(|r| evaluate_cuts(rt, &full, r, now).violated);

    let dropped: u32 = full.iter().map(|b| b.cuts.len() as u32).sum::<u32>()
        - pruned.iter().map(|b| b.cuts.len() as u32).sum::<u32>();
    let full_cuts: u32 = full.iter().map(|b| b.cuts.len() as u32).sum();
    let kept_cuts: u32 = pruned.iter().map(|b| b.cuts.len() as u32).sum();
    Some(PruneError {
        full_cuts,
        kept_cuts,
        dropped_cuts: dropped,
        // Kept cuts are constraints of the model that produced this solution, so they must not be
        // violated; anything they contribute is evaluator error and is reported separately rather
        // than being subtracted away and hidden.
        dropped_violated: full_eval.violated.saturating_sub(kept_violated),
        total_excess: full_eval.total_excess,
        max_excess: full_eval.max_excess,
        misplaced_tasks: full_eval.misplaced_tasks,
        kept_violated,
        skipped_multinode: full_eval.skipped_multinode,
        ref_assigned: reference.as_ref().map(|r| {
            r.sn_counts
                .values()
                .flat_map(|per_worker| per_worker.values())
                .sum()
        }),
        ref_is_optimal: reference.as_ref().map(|r| r.is_optimal),
        ref_n_constraints: reference.as_ref().map(|r| r.n_constraints),
        ref_violated,
        arm_objective: solution.objective,
        ref_objective: reference.as_ref().and_then(|r| r.objective),
    })
}

/// E8: run one what-if query on the current state and time it.
///
/// The query types are deliberately identical in shape to the real workers, which is the case the
/// autoallocator hits on a homogeneous cluster and the one that makes the fake workers genuinely
/// competitive with the real ones -- a query for a worker type nothing can use is trivially cheap
/// and would flatter the result. Resource names are kept stable across rounds because
/// `compute_new_worker_query` interns queried resource names into `Core` permanently
/// (`get_or_create_resource_id`), so varying them would grow the resource map over a sweep.
fn measure_query(rt: &mut TestEnv, spec: &SweepSpec) -> Option<QueryMeasurement> {
    if spec.query_workers == 0 {
        return None;
    }
    let queries: Vec<WorkerTypeQuery> = (0..spec.query_types.max(1))
        .map(|i| {
            let cpus = if spec.worker_types.is_empty() {
                spec.cpus_per_worker
            } else {
                spec.worker_types[i as usize % spec.worker_types.len()]
            };
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::new(
                    vec![ResourceDescriptorItem {
                        name: "cpus".to_string(),
                        kind: ResourceDescriptorKind::Sum {
                            size: ResourceAmount::new_units(cpus),
                        },
                    }],
                    Default::default(),
                ),
                time_limit: None,
                max_sn_workers: spec.query_workers,
                max_workers_per_allocation: spec.query_workers,
                min_utilization: 0.0,
                partial: spec.query_partial,
            }
        })
        .collect();

    let start = Instant::now();
    let (response, solve) = compute_new_worker_query_with_stats(rt.core(), &queries);
    let seconds = start.elapsed().as_secs_f64();
    Some(QueryMeasurement {
        seconds,
        n_variables: solve.n_variables,
        n_constraints: solve.n_constraints,
        is_optimal: solve.is_optimal,
        n_fake_workers: solve.n_fake_workers,
        reported: response.single_node_workers_per_query.iter().sum(),
    })
}

/// Complete a fraction of the tasks currently held by each worker, so the next round sees a
/// partially-occupied cluster. Also what E2 will need to walk a queue to exhaustion.
fn drain(rt: &mut TestEnv, worker_ids: &[WorkerId], fraction: f64) {
    if fraction <= 0.0 {
        return;
    }
    for &worker_id in worker_ids {
        let held: Vec<TaskId> = rt.worker_tasks(worker_id).iter().copied().collect();
        let take = ((held.len() as f64) * fraction).round() as usize;
        for task_id in held.into_iter().take(take) {
            // A round leaves tasks `Assigned`; prefilled ones are not runnable yet, and
            // anything already running was drained by an earlier pass.
            //
            // Report the variant the scheduler actually chose, not variant 0: with
            // multi-variant requests it may pick another, and `on_task_update` asserts the
            // running report matches the assignment (`reactor.rs:295`).
            if let TaskRuntimeState::Assigned { rv_id, .. } = rt.task(task_id).state {
                rt.start_task(task_id, rv_id);
                rt.finish_task(task_id, worker_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rq(id: u32) -> ResourceRqId {
        ResourceRqId::new(id)
    }

    fn prio(p: i32) -> Priority {
        Priority::from_user_priority(p.into())
    }

    /// `(request, priority) -> waiting count`
    fn buckets(items: &[(u32, i32, u32)]) -> Map<(ResourceRqId, Priority), u32> {
        items
            .iter()
            .map(|(r, p, n)| ((rq(*r), prio(*p)), *n))
            .collect()
    }

    fn requests(ids: &[u32]) -> Set<ResourceRqId> {
        ids.iter().map(|id| rq(*id)).collect()
    }

    #[test]
    fn test_inversions_none_selected() {
        let before = buckets(&[(0, 5, 3)]);
        let inv = measure_inversions(&before, &requests(&[0]), &before, &requests(&[0]));
        assert_eq!(inv.inverted, 0);
        assert_eq!(inv.min_inverted_rank, None);
    }

    #[test]
    fn test_inversions_high_priority_fully_selected_is_clean() {
        // Priority 5 drained entirely, then priority 1 ran: exactly what the rule permits.
        let before = buckets(&[(0, 5, 2), (0, 1, 4)]);
        let after = buckets(&[(0, 5, 0), (0, 1, 2)]);
        let inv = measure_inversions(&before, &requests(&[0]), &after, &requests(&[0]));
        assert_eq!(inv.inverted, 0);
        assert_eq!(inv.inverted_strict, 0);
        assert_eq!(inv.schedulable_levels, 2);
    }

    #[test]
    fn test_inversions_detects_real_violation() {
        // Priority 1 ran while priority 5 still had a task and its request still had room.
        let before = buckets(&[(0, 5, 2), (0, 1, 4)]);
        let after = buckets(&[(0, 5, 1), (0, 1, 2)]);
        let inv = measure_inversions(&before, &requests(&[0]), &after, &requests(&[0]));
        assert_eq!(inv.inverted, 1);
        assert_eq!(inv.min_inverted_rank, Some(0)); // the highest-priority level
    }

    /// The case that made the strict reading unusable: a priority level over-subscribes the
    /// cluster, so tasks must remain, and the leftover holes fit only a smaller request.
    #[test]
    fn test_inversions_ignores_unplaceable_remainder() {
        let before = buckets(&[(0, 5, 2), (1, 1, 4)]);
        let after = buckets(&[(0, 5, 1), (1, 1, 2)]);
        // Request 0 has no room left after the round; request 1 does.
        let inv = measure_inversions(&before, &requests(&[0, 1]), &after, &requests(&[1]));
        assert_eq!(inv.inverted, 0, "unplaceable remainder must not count");
        assert_eq!(
            inv.inverted_strict, 1,
            "but it is still visible in the strict count"
        );
        assert_eq!(inv.min_inverted_rank, None);
    }

    /// Rank is reported for the *highest-priority* violated level, which is what the paper's
    /// "the fixed prefix keeps the head exact" claim is tested against.
    #[test]
    fn test_inversions_rank_is_of_highest_violated_level() {
        let before = buckets(&[(0, 9, 1), (0, 7, 1), (0, 5, 1), (0, 1, 4)]);
        // 9 fully drained; 7 and 5 left with work; 1 ran anyway.
        let after = buckets(&[(0, 9, 0), (0, 7, 1), (0, 5, 1), (0, 1, 2)]);
        let inv = measure_inversions(&before, &requests(&[0]), &after, &requests(&[0]));
        assert_eq!(inv.inverted, 2);
        assert_eq!(inv.min_inverted_rank, Some(1)); // rank 0 is priority 9, which was clean
    }

    /// `kept_violated` is a self-check: cuts that survive pruning are constraints of the model
    /// that produced the solution, so the mirror must never report one as violated. Reservations
    /// are the case that used to break it -- a held worker counts toward the blocker in the model
    /// but has no entry in `sn_counts`, so a mirror reading placements alone saw a discharged
    /// condition as still binding.
    ///
    /// The scenario is the smallest one that discharges a condition with a reservation. Five
    /// 3-cpu workers, four already running a 1-cpu task, so a 3-cpu blocker cannot start on them
    /// now but could once they drain. Two blocker tasks are queued: one is placed on the free
    /// worker, one worker is held for the other, and the remaining three are then free for
    /// lower-priority work. See `test_scheduler_sn::test_reservation_one_assigned` for the
    /// scheduler-side statement of the same behaviour.
    #[test]
    fn test_evaluate_cuts_counts_reservations_as_serving_the_blocker() {
        let mut rt = TestEnv::new();
        rt.set_scheduler_config(SchedulerConfig {
            proactive_filling_max: 0,
            ..Default::default()
        });
        rt.new_worker(&WorkerBuilder::new(3));
        for _ in 0..4 {
            let w = rt.new_worker(&WorkerBuilder::new(3));
            rt.new_task_running(&TaskBuilder::new().cpus(1), w);
        }
        let blocker = rt.new_tasks(2, &TaskBuilder::new().cpus(3).user_priority(10));
        rt.new_tasks(8, &TaskBuilder::new().cpus(1).user_priority(5));

        let now = rt.now();
        let batches = create_task_batches(rt.core(), now, None);
        let solution = run_scheduling_solver(rt.core(), now, &batches, None);

        // Without these the assertion below could pass vacuously: the condition has to be one
        // that *only* the reservation discharges, i.e. one the blocker's placements alone leave
        // binding. 1 placed + 1 held = the 2 pending tasks the condition asks for.
        let blocker_rq = rt.task(blocker[0]).resource_rq_id;
        let blocker_placed: u32 = solution
            .sn_counts
            .iter()
            .filter(|((rq_id, _), _)| *rq_id == blocker_rq)
            .flat_map(|(_, per_worker)| per_worker.values())
            .sum();
        let reserved: u32 = solution.reserved_counts.values().sum();
        assert_eq!(
            (blocker_placed, reserved, blocker.len()),
            (1, 1, 2),
            "scenario changed: it must place one blocker task, hold one worker for the other, \
             and leave the condition binding on placements alone"
        );

        let eval = evaluate_cuts(&mut rt, &batches, &solution, now);
        let (violated, total_excess) = (eval.violated, eval.total_excess);
        assert_eq!(
            violated, 0,
            "kept cuts are constraints of the model that produced this solution, so none may be \
             violated; got {violated} with total excess {total_excess}"
        );
    }

    /// `ref_violated` is the reference's own self-check, and the analogue of `kept_violated`: an
    /// *optimal* solution to the unpruned model satisfies every condition in that model, so a
    /// non-zero count here means `evaluate_cuts` has diverged from `solver.rs` and every quality
    /// number in the row is void. It is worth pinning separately because it exercises the
    /// evaluator against a different solution than the arm's.
    ///
    /// Also pins the plumbing either way: the fields are `None` unless `reference_solve` is set,
    /// so a silently-disabled reference would read as "not measured" rather than as zero.
    #[test]
    fn test_reference_solve_satisfies_its_own_conditions() {
        // Enough interleaving to generate conditions, small enough that the unpruned model solves
        // in milliseconds -- this pins the invariant, not the cost.
        // Heavy interleaving with few tasks per level, which is what actually generates
        // conditions (see the sweep README): many priority levels over a cluster with enough free
        // slots that the batch walk is not cut short.
        let spec = SweepSpec {
            n_workers: 16,
            cpus_per_worker: 8,
            n_request_types: 3,
            priority_levels: 60,
            n_tasks: 300,
            rounds: 1,
            prefill_max: 0,
            prune_global_max: 6,
            check_pruning: true,
            reference_solve: true,
            ..Default::default()
        };
        let rows = run_sweep(&spec);
        let error = rows[0]
            .prune_error
            .as_ref()
            .expect("check_pruning was set, so the error must be measured");

        assert!(
            error.dropped_cuts > 0,
            "the budget must actually prune, or the reference is the same model and the check is              vacuous: {} full, {} kept",
            error.full_cuts,
            error.kept_cuts
        );
        assert_eq!(
            error.ref_is_optimal,
            Some(true),
            "the reference must prove optimality on a cell this small, or the invariant below              cannot be asserted"
        );
        assert_eq!(
            error.ref_violated,
            Some(0),
            "an optimal solution to the unpruned model satisfies every condition in it; a              non-zero count means the evaluator disagrees with the solver"
        );
        assert!(
            error.ref_assigned.is_some() && error.ref_n_constraints.is_some(),
            "reference columns must be populated when the flag is set"
        );
    }

    /// The solver is given only a time limit (`solver/highs.rs:67`), so HiGHS's default
    /// `mip_rel_gap` of 1e-4 applies: `HighsModelStatus::Optimal` means "proved within 1e-4
    /// relative", **not** exact. Two independent solves can each sit that far off the true optimum
    /// in opposite directions, so objective comparisons between them are only meaningful to about
    /// twice that. Measured violations of the relaxation inequality on real sweeps reach 8.4e-5
    /// relative, comfortably inside this bound and comfortably outside a naive 1e-9 one.
    const MIP_GAP_TOL: f64 = 2e-4;

    /// `misplaced_tasks` is the only priority-error number in this file with a denominator, so the
    /// properties that make it one are worth pinning rather than trusting.
    ///
    /// The fourth assertion is the important one: the dedup could "pass" by simply reproducing
    /// `total_excess`, so a cell where it strictly bites has to be part of the test. (A test two
    /// steps ago passed vacuously for exactly this kind of reason.)
    #[test]
    fn test_misplaced_tasks_is_a_deduplicated_count() {
        let rows: Vec<_> = [6, 16, 32]
            .into_iter()
            .flat_map(|prune_global_max| {
                run_sweep(&SweepSpec {
                    n_workers: 16,
                    cpus_per_worker: 8,
                    n_request_types: 3,
                    priority_levels: 60,
                    n_tasks: 300,
                    rounds: 3,
                    prefill_max: 0,
                    prune_global_max,
                    check_pruning: true,
                    ..Default::default()
                })
            })
            .collect();

        let mut dedup_bit = false;
        for row in &rows {
            let e = row.prune_error.as_ref().unwrap();
            let assigned = row.stats.n_tasks_assigned;

            // The dedup can only remove double counting, never add.
            assert!(
                e.misplaced_tasks <= e.total_excess,
                "misplaced {} exceeds the total excess {} it deduplicates",
                e.misplaced_tasks,
                e.total_excess
            );
            // It counts dispatched tasks, so it cannot exceed what was dispatched. This is what
            // makes the fraction meaningful, and it is what fails if two batches ever claim the
            // same placement.
            assert!(
                e.misplaced_tasks <= assigned,
                "misplaced {} exceeds the {assigned} tasks actually dispatched",
                e.misplaced_tasks
            );
            // The dedup must lose duplication, not signal.
            assert_eq!(
                e.misplaced_tasks == 0,
                e.dropped_violated == 0,
                "misplaced {} and dropped_violated {} disagree about whether anything was \
                 misplaced",
                e.misplaced_tasks,
                e.dropped_violated
            );
            dedup_bit |= e.misplaced_tasks < e.total_excess;
        }

        assert!(
            dedup_bit,
            "no row had overlapping conditions, so the deduplication was never exercised -- this \
             test would pass on an implementation that simply returned total_excess"
        );
    }

    /// Pruning is a *relaxation*: it only ever deletes conditions, so the pruned model's feasible
    /// set is a superset of the full model's, and every backend maximises
    /// (`solver/highs.rs:52`). The pruned optimum therefore can never score *worse* than the
    /// unpruned one. This holds on every row where both solves are optimal, whatever the
    /// violations, so it catches an inverted sign or a missing objective term earlier and on more
    /// rows than the equality theorem below.
    #[test]
    fn test_pruned_optimum_is_never_worse_than_unpruned() {
        for prune_global_max in [4, 6, 8, 16] {
            let spec = SweepSpec {
                n_workers: 16,
                cpus_per_worker: 8,
                n_request_types: 3,
                priority_levels: 60,
                n_tasks: 300,
                rounds: 2,
                prefill_max: 0,
                prune_global_max,
                check_pruning: true,
                reference_solve: true,
                ..Default::default()
            };
            for row in run_sweep(&spec) {
                let e = row.prune_error.as_ref().unwrap();
                if !row.stats.is_optimal || e.ref_is_optimal != Some(true) {
                    continue;
                }
                let (arm, reference) = (e.arm_objective.unwrap(), e.ref_objective.unwrap());
                assert!(
                    arm >= reference - MIP_GAP_TOL * reference.abs().max(1.0),
                    "a relaxed model scored worse than the model it relaxes: arm {arm} < \
                     reference {reference} (G = {prune_global_max})"
                );
            }
        }
    }

    /// The sharp case: when the arm's solution violates *none* of the dropped conditions it is
    /// feasible in the full model, so its objective is also `<= ref_objective`. With the
    /// inequality above that forces **equality** -- the arm found an alternative optimum, a
    /// different argmax rather than a better schedule.
    ///
    /// So "aggressive pruning can be strictly better than no pruning" cannot be true as stated:
    /// more tasks at an equal objective is a different distribution of the same value.
    #[test]
    fn test_zero_violations_means_an_alternative_optimum() {
        // Sweep the budget rather than fixing it: whether a round both prunes and violates
        // nothing depends on the state the previous round left behind, so no single G reliably
        // produces a qualifying row.
        let rows: Vec<_> = [4, 6, 8, 16, 32]
            .into_iter()
            .flat_map(|prune_global_max| {
                run_sweep(&SweepSpec {
                    n_workers: 16,
                    cpus_per_worker: 8,
                    n_request_types: 3,
                    priority_levels: 60,
                    n_tasks: 300,
                    rounds: 3,
                    prefill_max: 0,
                    prune_global_max,
                    check_pruning: true,
                    reference_solve: true,
                    ..Default::default()
                })
            })
            .collect();

        let mut checked = 0;
        for row in rows {
            let e = row.prune_error.as_ref().unwrap();
            if !row.stats.is_optimal
                || e.ref_is_optimal != Some(true)
                || e.dropped_violated != 0
                || e.dropped_cuts == 0
            {
                continue;
            }
            let (arm, reference) = (e.arm_objective.unwrap(), e.ref_objective.unwrap());
            assert!(
                (arm - reference).abs() <= MIP_GAP_TOL * reference.abs().max(1.0),
                "no dropped condition is violated, so the arm's solution is feasible in the full \
                 model and must share its optimum: arm {arm} vs reference {reference}"
            );
            assert!(
                reference.abs() > 0.0,
                "a zero objective would make this vacuous"
            );
            checked += 1;
        }
        assert!(
            checked > 0,
            "no row pruned, solved optimally and violated nothing, so the theorem was never \
             exercised -- pick a cell where it is"
        );
    }

    /// The reference must not perturb what it measures: a measured quantity that changes when you
    /// measure it is not a measurement. `run_scheduling_solver` does not mutate `Core`, but that
    /// is the kind of thing to check rather than argue.
    #[test]
    fn test_reference_solve_does_not_change_the_arm() {
        let base = SweepSpec {
            n_workers: 16,
            cpus_per_worker: 8,
            n_request_types: 3,
            priority_levels: 60,
            n_tasks: 300,
            rounds: 2,
            prefill_max: 0,
            prune_global_max: 6,
            check_pruning: true,
            ..Default::default()
        };
        let without = run_sweep(&base);
        let with = run_sweep(&SweepSpec {
            reference_solve: true,
            ..base
        });

        for (a, b) in without.iter().zip(with.iter()) {
            let (ea, eb) = (
                a.prune_error.as_ref().unwrap(),
                b.prune_error.as_ref().unwrap(),
            );
            assert_eq!(
                (
                    a.stats.n_tasks_assigned,
                    a.stats.n_constraints,
                    ea.dropped_violated,
                    ea.total_excess
                ),
                (
                    b.stats.n_tasks_assigned,
                    b.stats.n_constraints,
                    eb.dropped_violated,
                    eb.total_excess
                ),
                "the reference solve changed the arm it was measuring"
            );
            assert_eq!(
                ea.ref_assigned, None,
                "reference must be off in the control"
            );
        }
    }
}
