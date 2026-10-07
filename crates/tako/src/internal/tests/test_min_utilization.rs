use crate::control::WorkerTypeQuery;
use crate::internal::scheduler::query::compute_new_worker_query;
use crate::internal::tests::utils::env::TestEnv;
use crate::internal::tests::utils::task::TaskBuilder;
use crate::internal::tests::utils::worker::WorkerBuilder;
use crate::resources::ResourceDescriptor;

/// Worker shape. The report used $128$ CPUs; only the ratio matters here.
const WORKER_CPUS: u32 = 8;

/// Place `n_small` one-CPU tasks and `n_big` whole-worker tasks on one worker that demands full
/// utilization, and report how many of each the round assigned.
fn place(
    n_small: usize,
    n_big: usize,
    weight: f32,
    prio_small: i32,
    prio_big: i32,
) -> (usize, usize) {
    let mut rt = TestEnv::new();
    rt.new_worker(&WorkerBuilder::new(WORKER_CPUS).min_utilization(1.0));
    let small = rt.new_tasks(
        n_small,
        &TaskBuilder::new()
            .cpus(1)
            .weight(weight)
            .user_priority(prio_small),
    );
    let big = rt.new_tasks(
        n_big,
        &TaskBuilder::new().cpus_all().user_priority(prio_big),
    );
    rt.schedule();
    (
        small.iter().filter(|t| rt.task(**t).is_assigned()).count(),
        big.iter().filter(|t| rt.task(**t).is_assigned()).count(),
    )
}

/// How many workers of `WORKER_CPUS` CPUs the scheduler says it could use.
fn workers_wanted(rt: &mut TestEnv, min_utilization: f32, max_sn_workers: u32) -> u32 {
    let response = compute_new_worker_query(
        rt.core(),
        &[WorkerTypeQuery {
            partial: false,
            descriptor: ResourceDescriptor::simple_cpus(WORKER_CPUS),
            time_limit: None,
            max_sn_workers,
            max_workers_per_allocation: 1,
            min_utilization,
        }],
    );
    response.single_node_workers_per_query[0]
}

#[test]
fn test_priority_expresses_precedence_and_therefore_blocks() {
    // A full batch runs, as it would with any expression of the intent.
    assert_eq!(place(8, 1, 1.0, 10, 0), (8, 0));
    // A partial batch cannot use the worker, and its priority forbids the whole-worker task from
    // using it either. The worker idles. This is the strict rule, not a defect.
    assert_eq!(place(3, 1, 1.0, 10, 0), (0, 0));
}

#[test]
fn test_weight_expresses_preference_without_blocking() {
    // Same intent, expressed as a weight at equal priority: the one-CPU work still wins the
    // worker whenever it can fill it ...
    assert_eq!(place(8, 1, 2.0, 0, 0), (8, 0));
    // ... and when it cannot, the whole-worker task keeps the worker busy instead of idling it.
    assert_eq!(place(3, 1, 2.0, 0, 0), (0, 1));
    // The preference does not depend on how strongly the weight is set: it is not a priority.
    assert_eq!(place(3, 1, 8.0, 0, 0), (0, 1));
}

#[test]
fn test_weight_is_a_real_knob() {
    // A placement is worth its share of the cluster's resources times its weight. A full batch of
    // eight one-CPU tasks of weight `w` is worth `8 * 1/8 * w = w`; the whole-worker task, at the
    // default weight, is worth `1`. The knob therefore flips the worker between the two mixes at
    // `w = 1`, rather than merely reinforcing a default.

    // Below one, the full batch loses the worker to the whole-worker task ...
    assert_eq!(place(8, 1, 0.5, 0, 0), (0, 1));
    assert_eq!(place(8, 1, 0.9, 0, 0), (0, 1));
    // ... and above one it wins it.
    assert_eq!(place(8, 1, 1.1, 0, 0), (8, 0));
    assert_eq!(place(8, 1, 2.0, 0, 0), (8, 0));

    // At exactly one the two mixes are worth the same, and which one an LP backend returns is
    // its own tie-break (HiGHS and Cbc differ). Only what both optima share is asserted: the
    // worker is filled by one mix or the other, never split or left idle.
    let at_tie = place(8, 1, 1.0, 0, 0);
    assert!(
        at_tie == (8, 0) || at_tie == (0, 1),
        "expected one full mix at the tie, got {at_tie:?}"
    );
}

/// How many workers the allocation queue would ask for, in the same two states.
fn wanted(n_small: usize, n_big: usize, weight: f32, prio_small: i32, prio_big: i32) -> u32 {
    let mut rt = TestEnv::new();
    rt.new_tasks(
        n_small,
        &TaskBuilder::new()
            .cpus(1)
            .weight(weight)
            .user_priority(prio_small),
    );
    rt.new_tasks(
        n_big,
        &TaskBuilder::new().cpus_all().user_priority(prio_big),
    );
    workers_wanted(&mut rt, 1.0, 4)
}

#[test]
fn test_allocation_queue_follows_the_same_two_expressions() {
    // Eight one-CPU tasks and one whole-worker task: the one-CPU work can fill a worker, so
    // nothing is blocked and both expressions ask for the two workers the queue can use.
    assert_eq!(wanted(8, 1, 1.0, 10, 0), 2);
    assert_eq!(wanted(8, 1, 2.0, 0, 0), 2);

    // Three one-CPU tasks and one whole-worker task: the expressions part company. The weight
    // asks for the one worker the filler can use; the priority asks for nothing, which is the
    // idle worker of `test_priority_expresses_precedence_and_therefore_blocks` one round earlier.
    assert_eq!(wanted(3, 1, 2.0, 0, 0), 1);
    assert_eq!(wanted(3, 1, 1.0, 10, 0), 0);

    // The weight expression tracks the amount of filler, the priority expression stays at zero.
    assert_eq!(wanted(3, 2, 2.0, 0, 0), 2);
    assert_eq!(wanted(3, 2, 1.0, 10, 0), 0);

    // One-CPU tasks on their own ask only for the workers they can fill, under either expression.
    let mut rt = TestEnv::new();
    rt.new_tasks(3, &TaskBuilder::new().cpus(1).weight(2.0));
    assert_eq!(workers_wanted(&mut rt, 1.0, 4), 0);

    let mut rt = TestEnv::new();
    rt.new_tasks(16, &TaskBuilder::new().cpus(1).weight(2.0));
    assert_eq!(workers_wanted(&mut rt, 1.0, 4), 2);
}
