use crate::internal::scheduler::mapping::create_task_mapping;
use crate::internal::scheduler::{create_task_batches, run_scheduling_solver};
use crate::internal::server::comm::{Comm, CommSender, CommSenderRef};
use crate::internal::server::core::{Core, CoreRef};
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;

pub(crate) async fn scheduler_loop(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    scheduler_wakeup: Rc<Notify>,
    minimum_delay: Duration,
) {
    let mut last_schedule = Instant::now().checked_sub(minimum_delay * 2).unwrap();
    loop {
        scheduler_wakeup.notified().await;
        let mut now = Instant::now();
        if !comm_ref.get().get_scheduling_flag() {
            last_schedule = now;
            continue;
        }
        let since_last_schedule = now - last_schedule;
        if minimum_delay > since_last_schedule {
            sleep(minimum_delay - since_last_schedule).await;
            now = Instant::now();
        }
        if !comm_ref.get_mut().get_scheduling_flag() {
            last_schedule = now;
            continue;
        }
        while matches!(
            run_scheduling(&mut core_ref.get_mut(), &mut comm_ref.get_mut(), now),
            SchedulerResult::NeedMoreCompute
        ) {
            sleep(minimum_delay).await;
        }
        comm_ref.get_mut().reset_scheduling_flag();
        last_schedule = Instant::now();
    }
}

pub(crate) enum SchedulerResult {
    Done,
    NeedMoreCompute,
    NoProgress,
}

/// Per-round scheduler measurements, for the paper's evaluation (`benchmarks/paper/`).
///
/// Populated on every round; the counters behind it are plain integer increments in the
/// model-building loops, so this is cheap enough to leave always on. Returned from
/// `run_scheduling_inner` as well as logged, so the in-process simulation harness can assert
/// on it without parsing logs.
#[derive(Debug, Default, Clone)]
pub struct SchedulerRoundStats {
    pub n_workers: u32,
    pub n_batches: u32,
    /// Sum of batch sizes, i.e. tasks the solver was allowed to consider this round.
    pub n_tasks_considered: u32,
    /// Tasks actually placed by the solution.
    pub n_tasks_assigned: u32,
    /// CPUs consumed by the placed tasks, and CPUs that were free before the round. Their ratio is
    /// the share of the cluster the round managed to put to work, which is what a truncated solve
    /// costs in the currency an operator cares about.
    pub cpus_assigned: f64,
    pub cpus_free: f64,
    /// Workers this round placed at least one task on. Against `n_workers` it says how widely the
    /// round spread the same work, which is what the compaction bias of the objective trades
    /// against utilization -- two solutions can use the same CPUs on a different number of nodes.
    pub n_workers_used: u32,
    /// The MILP objective of the dispatched solution, or `None` where the terms are not recorded.
    /// Utilization cannot see a solution that merely packs the same work better.
    pub objective: Option<f64>,
    /// MILP size. The key E1 claim is that these do not grow with the number of
    /// waiting tasks, only with workers x resource requests.
    pub n_variables: u32,
    pub n_constraints: u32,
    /// Priority conditions before and after `prune_progressive` (E2).
    pub n_cuts_before_prune: u32,
    pub n_cuts_after_prune: u32,
    pub gap_cache_hits: u32,
    pub gap_cache_misses: u32,
    /// Prefilled tasks retracted because the solver placed them on another worker.
    pub n_prefill_replaced: u32,
    /// Prefilled tasks dumped because a strictly higher priority arrived, accumulated since the
    /// previous round (disposal happens on submit, not during a round).
    pub n_prefill_disposed: u32,
    pub is_optimal: bool,
    pub t_batches: Duration,
    pub t_solve: Duration,
    pub t_mapping: Duration,
    pub t_send: Duration,
}

impl SchedulerRoundStats {
    pub fn total_time(&self) -> Duration {
        self.t_batches + self.t_solve + self.t_mapping + self.t_send
    }
}

pub(crate) fn run_scheduling_inner(
    core: &mut Core,
    comm: &mut impl Comm,
    now: Instant,
) -> (SchedulerResult, SchedulerRoundStats) {
    let mut stats = SchedulerRoundStats::default();
    core.split().scheduler_state.gap_cache.reset_counters();

    let t = Instant::now();
    let batches = trace_time!("scheduler", "create_task_batches", {
        create_task_batches(core, now, None)
    });
    stats.t_batches = t.elapsed();
    stats.n_batches = batches.len() as u32;
    stats.n_tasks_considered = batches.iter().map(|b| b.size).sum();
    stats.n_cuts_before_prune = batches.iter().map(|b| b.cuts_before_prune).sum();
    stats.n_cuts_after_prune = batches.iter().map(|b| b.cuts.len() as u32).sum();

    let t = Instant::now();
    let solution = trace_time!("scheduler", "run_scheduling_solver", {
        run_scheduling_solver(core, now, &batches, None)
    });
    stats.t_solve = t.elapsed();
    stats.n_variables = solution.n_variables;
    stats.n_constraints = solution.n_constraints;
    stats.is_optimal = solution.is_optimal;
    stats.n_tasks_assigned = solution
        .sn_counts
        .values()
        .flat_map(|m| m.values())
        .sum::<u32>();
    stats.objective = solution.objective;
    stats.n_workers_used = solution
        .sn_counts
        .values()
        .flat_map(|per_worker| per_worker.iter())
        .filter(|(_, count)| **count > 0)
        .map(|(w_id, _)| *w_id)
        .collect::<crate::Set<_>>()
        .len() as u32;
    {
        let split = core.split();
        stats.cpus_assigned = solution
            .sn_counts
            .iter()
            .map(|((rq_id, v_id), per_worker)| {
                let rq = split.request_map.get(*rq_id).get(*v_id);
                let cpus = rq
                    .get_amount(crate::resources::CPU_RESOURCE_ID)
                    .map(|a| a.as_f64())
                    .unwrap_or(0.0);
                cpus * per_worker.values().sum::<u32>() as f64
            })
            .sum();
        stats.cpus_free = split
            .worker_map
            .get_workers()
            .filter_map(|w| w.sn_assignment())
            .map(|a| {
                a.free_resources
                    .get(crate::resources::CPU_RESOURCE_ID)
                    .as_f64()
            })
            .sum();
    }
    (stats.gap_cache_hits, stats.gap_cache_misses) =
        core.split().scheduler_state.gap_cache.counters();
    stats.n_prefill_disposed = core.split().task_queues.prefill_disposed();
    core.split_mut().task_queues.reset_prefill_disposed();

    let need_more_compute = if !solution.is_optimal {
        if solution.is_empty() {
            log::error!("Scheduler made no progress within given time limit");
            SchedulerResult::NoProgress
        } else {
            log::debug!("Scheduler dispatched a non-optimal placement this round");
            SchedulerResult::NeedMoreCompute
        }
    } else {
        SchedulerResult::Done
    };

    let t = Instant::now();
    let mapping = trace_time!("scheduler", "create_task_mapping", {
        create_task_mapping(core, solution)
    });
    stats.t_mapping = t.elapsed();
    stats.n_prefill_replaced = mapping
        .workers
        .values()
        .map(|up| up.retracts.len() as u32)
        .sum();
    //mapping.dump();

    let t = Instant::now();
    trace_time!("scheduler", "send_messages", {
        mapping.send_messages(core, comm)
    });
    stats.t_send = t.elapsed();

    stats.n_workers = core.get_workers().count() as u32;
    log::debug!(
        "Scheduler round ({:?} total): {stats:?}",
        stats.total_time()
    );
    (need_more_compute, stats)
}

pub(crate) fn run_scheduling(
    core: &mut Core,
    comm: &mut CommSender,
    now: Instant,
) -> SchedulerResult {
    trace_time!(
        "scheduler",
        "run_scheduling_inner",
        run_scheduling_inner(core, comm, now).0
    )
}
