#!/usr/bin/env python3
"""Worked examples of `prune_misplaced_tasks`, the priority-error metric E2 quotes as a share.

The metric is one division -- `prune_misplaced_tasks / n_tasks_assigned` -- but the numerator is a
reduction with several collapsing steps, and the phrase "share of dispatched tasks placed in
violation of a dropped condition" hides all of them. These are that reduction on rounds small
enough to check by hand, printed step by step.

Both mirror `evaluate_cuts` in `crates/tako/src/internal/sim.rs` on the paths a
single-node workload takes:

  * a **discharged blocker is skipped** -- if the blocker got what the condition asked for, placed
    *or* reserved, the condition was not binding in the model either, and scoring it as violated
    would report an error the solver never made;
  * per cut, a worker with a positive **gap** carries a per-worker constraint, so its excess is
    `placed - (cut.size + gap)` and the cut takes the **maximum** over such workers; workers with a
    zero gap share one joint constraint, so they are **summed** and the sum takes `- cut.size`;
  * per batch, only the **tightest violated cut** contributes. Cuts in a batch are generated at
    increasing `cut.size` over the same request, so their task sets are nested and summing them
    would count the same placements repeatedly;
  * across batches, the per-batch maxima are **summed** -- safe because batches are distinct
    requests and placements are keyed `(batch request, worker)`, so no two can claim the same one.

Scenario 1 is the smallest round that makes the metric legible: two requests, one worker, and a
priority order whose conditions admit exactly one correct schedule. Scenario 2 adds what the first
cannot show -- a positive gap, nested cuts that both fire, and the cross-batch sum.

Usage:
    python3 misplaced_example.py
"""

from dataclasses import dataclass, field


@dataclass
class Cut:
    """One priority condition: at most `size` tasks of the batch's request may run where a blocker
    is waiting. `blockers` is `(blocker request, blocking size)` -- the condition is discharged once
    the blocker has `blocking size` tasks placed or reserved."""

    name: str
    size: int
    blockers: list[tuple[str, int]]


@dataclass
class Batch:
    """One lower-priority resource request, with the conditions holding it back."""

    request: str
    cuts: list[Cut] = field(default_factory=list)


@dataclass
class Scenario:
    name: str
    intro: str
    workers: list[str]
    #: `placed[request][worker]`: tasks of that request this round's solution placed there.
    placed: dict[str, dict[str, int]]
    #: Reservations the solution bought, per request. These count toward discharging a blocker:
    #: the model's blocking variable sums placements and reservations alike.
    reserved: dict[str, int]
    #: `gap[(blocker, batch request)][worker]`: how many tasks of the batch's request may run on
    #: that worker without denying the blocker (relaxation 2). Zero means the worker joins the
    #: joint constraint instead of carrying its own.
    gap: dict[tuple[str, str], dict[str, int]]
    batches: list[Batch]
    #: `n_tasks_assigned`: what the round dispatched, and the denominator. One round, not the job.
    assigned: int
    #: True where the per-batch maximum must strictly bite, i.e. two cuts of one batch both fire.
    expect_dedup: bool
    outro: str


# --- Scenario 1 ---------------------------------------------------------------------------------
#
# Two 1-cpu tasks per priority level, one worker of 6 cpus:
#
#     R1 at priority levels 5, 3, 1      R2 at priority levels 4, 2
#
# The merge walk over the two queues emits a condition at every priority boundary, into the batch
# it constrains, naming as blocker whichever batch has already accumulated tasks:
#
#     X1  #R2 > 0  ->  #R1 >= 2     on R2, size 0     (before R2's level 4, R1 has 2)
#     X2  #R1 > 2  ->  #R2 >= 2     on R1, size 2     (before R1's level 3, R2 has 2)
#     X3  #R2 > 2  ->  #R1 >= 4     on R2, size 2     (before R2's level 2, R1 has 4)
#     X4  #R1 > 4  ->  #R2 >= 4     on R1, size 4     (before R1's level 1, R2 has 4)
#
# Those four admit exactly one full schedule -- #R1 = 4, #R2 = 2, which is R1@5, R2@4, R1@3, the
# true priority order. Prune X3 and two more become reachable at the identical objective (every
# task is 1 cpu, so all three place 6): #R1 = 3, #R2 = 3, and the one below.
#
# The gap is 0 everywhere and worth checking, because it is not obvious: `get_gap` packs the worker
# with blocker tasks first (6 / 1 = 6, leaving nothing), then takes the remainder `capacity mod r`
# = 6 mod 1 = 0. With 1-cpu blockers on a 6-cpu worker there is no space the blocker cannot use.

SCENARIO_1 = Scenario(
    name="Two requests, one worker -- what the number means",
    intro="R1 at priority levels 5/3/1, R2 at 4/2, two 1-cpu tasks each, one 6-cpu worker.\n"
    "X3 was pruned, and the solver returned #R1=2, #R2=4 -- one of the three schedules\n"
    "that becomes reachable once X3 is gone, and the worst of them.",
    workers=["w"],
    placed={"R1": {"w": 2}, "R2": {"w": 4}},
    reserved={},
    gap={("R1", "R2"): {"w": 0}, ("R2", "R1"): {"w": 0}},
    batches=[
        Batch("R1", [
            Cut("X2", size=2, blockers=[("R2", 2)]),
            Cut("X4", size=4, blockers=[("R2", 4)]),
        ]),
        Batch("R2", [
            Cut("X1", size=0, blockers=[("R1", 2)]),
            Cut("X3", size=2, blockers=[("R1", 4)]),
        ]),
    ],
    assigned=6,
    expect_dedup=False,
    outro="""The 2 is not a coincidence of arithmetic. X3's `size` is 2 because two R2 tasks --
its priority-4 pair -- sit above the boundary where the condition was created, so they are
allowed. The excess 4 - 2 counts exactly R2's priority-2 pair: the tasks that ran while R1's
priority-3 pair did not. The metric and the eye land on the same two tasks.

Note also that the evaluator runs over the *full* condition set, not the dropped one. X1, X2
and X4 were kept, and all three come back discharged -- which is the self-check: kept conditions
were constraints of the model that produced this solution, so a correct evaluator must never
find one violated. `dropped_violated` is then 1 - 0 = 1.""",
)

# --- Scenario 2 ---------------------------------------------------------------------------------
#
# Three workers, and everything scenario 1 has no room for: a worker with a positive gap, a batch
# whose nested cuts both fire, a blocker that was discharged by a *reservation*, and two batches
# contributing to the sum.

SCENARIO_2 = Scenario(
    name="Three workers -- the parts a two-request round cannot show",
    intro="`H` is blocked with one reservation; `L` is a second blocker that was served.\n"
    "`A` and `B` are the lower-priority requests the round dispatched.",
    workers=["w1", "w2", "w3"],
    placed={
        "A": {"w1": 6, "w2": 3, "w3": 2},   # 11 tasks
        "B": {"w1": 0, "w2": 5, "w3": 4},   #  9 tasks
        "H": {"w1": 0, "w2": 0, "w3": 0},   #  blocked
        "L": {"w1": 2, "w2": 0, "w3": 0},   #  served
    },
    reserved={"H": 1, "L": 0},
    gap={
        ("H", "A"): {"w1": 1, "w2": 0, "w3": 0},
        ("H", "B"): {"w1": 1, "w2": 0, "w3": 0},
        ("L", "B"): {"w1": 0, "w2": 0, "w3": 0},
    },
    batches=[
        Batch("A", [
            # Nested: same request, increasing size. Both fire, which is what makes the per-batch
            # maximum do visible work.
            Cut("A1", size=2, blockers=[("H", 3)]),
            Cut("A2", size=4, blockers=[("H", 3)]),
        ]),
        Batch("B", [
            Cut("B1", size=3, blockers=[("H", 3)]),
            # Discharged: `L` has 2 placed against a blocking size of 2.
            Cut("B2", size=3, blockers=[("L", 2)]),
        ]),
    ],
    assigned=20,
    expect_dedup=True,
    outro="""Two readings the phrase "tasks placed in violation" invites, and neither is right:

  * It does not name tasks. Cut A1 allows w1 three A-tasks and got six; the excess is 3, but
    *which* three jumped the queue is not determined, and nothing in the metric decides it.
  * It is a floor, not a total. Under A1, w1 overshoots by 3 and the joint constraint on w2+w3
    overshoots by 3 as well; the cut still reports 3, because it takes the worst single broken
    constraint rather than adding them. The same maximum then applies across a batch's nested
    cuts. Both steps trade an overcount for an undercount, deliberately: the alternative,
    `total_excess`, has exceeded the round's dispatched task count by 16x on real data, which
    no quantity called a share can survive.""",
)

SCENARIOS = [SCENARIO_1, SCENARIO_2]


def cut_excess(s: Scenario, batch: Batch, cut: Cut, log) -> int:
    """The overshoot of one condition: the worst single constraint it breaks, or 0 if none."""
    worst = 0
    for blocker, blocking_size in cut.blockers:
        served = sum(s.placed[blocker].values()) + s.reserved.get(blocker, 0)
        if served >= blocking_size:
            log(f"    blocker {blocker}: {served} placed+reserved >= {blocking_size} asked "
                f"-> discharged, condition skipped")
            continue
        log(f"    blocker {blocker}: {served} placed+reserved < {blocking_size} asked -> binding")
        zero_gap_sum = 0
        for w in s.workers:
            gap = s.gap[(blocker, batch.request)][w]
            placed = s.placed[batch.request][w]
            if gap > 0:
                excess = max(0, placed - (cut.size + gap))
                log(f"      {w}: gap {gap} > 0, own constraint: "
                    f"{placed} placed - ({cut.size} + {gap}) = {excess}")
                worst = max(worst, excess)
            else:
                zero_gap_sum += placed
                log(f"      {w}: gap 0, joins the joint constraint (+{placed})")
        joint = max(0, zero_gap_sum - cut.size)
        log(f"      joint: {zero_gap_sum} placed - {cut.size} = {joint}")
        # The maximum, not the sum: each constraint is broken by its own amount, and the metric
        # reports the worst rather than adding overlapping evidence together.
        worst = max(worst, joint)
    return worst


def evaluate(s: Scenario) -> None:
    print("\n" + "=" * 78)
    print(s.name)
    print("=" * 78)
    print(s.intro)

    total_excess = violated = misplaced_tasks = 0
    for batch in s.batches:
        print(f"\nbatch {batch.request}: {sum(s.placed[batch.request].values())} tasks placed "
              f"({', '.join(f'{w}={s.placed[batch.request][w]}' for w in s.workers)})")
        batch_worst = 0
        for cut in batch.cuts:
            print(f"  cut {cut.name} (size {cut.size}):")
            excess = cut_excess(s, batch, cut, log=print)
            if excess > 0:
                violated += 1
                total_excess += excess
                # The tightest violated cut subsumes the rest: its task set contains theirs.
                batch_worst = max(batch_worst, excess)
                print(f"    -> VIOLATED by {excess}")
            else:
                print("    -> satisfied")
        print(f"  batch {batch.request} contributes max(...) = {batch_worst}, not the sum")
        misplaced_tasks += batch_worst

    print(f"\n  dropped_violated = {violated}   (conditions broken; grows with throughput)")
    print(f"  total_excess     = {total_excess}   (sums nested cuts -- double counts)")
    print(f"  misplaced_tasks  = {misplaced_tasks}   (per-batch maxima, summed across batches)")
    print(f"  n_tasks_assigned = {s.assigned}")
    print(f"  share            = {misplaced_tasks}/{s.assigned} "
          f"= {misplaced_tasks / s.assigned:.1%} of dispatched tasks")

    # The properties `test_misplaced_tasks_is_a_deduplicated_count` pins in Rust.
    assert misplaced_tasks <= total_excess, "the dedup added rather than removed"
    assert misplaced_tasks <= s.assigned, "more misplaced than dispatched: batches overlap"
    assert (misplaced_tasks == 0) == (violated == 0), "the dedup lost signal, not duplication"
    if s.expect_dedup:
        assert misplaced_tasks < total_excess, "the per-batch maximum never bit in this scenario"
    print(f"\n{s.outro}")


def main() -> int:
    print(__doc__.split("Usage:")[0].strip())
    for scenario in SCENARIOS:
        evaluate(scenario)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
