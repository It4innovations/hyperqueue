"""Scenario definitions for the end-to-end experiments of the paper.

A scenario is a timeline of events, each with an offset in seconds relative to
the moment the server became ready. Two kinds of events exist:

* ``StartWorkers`` -- spawn N workers with a given number of (virtual) CPUs
* ``SubmitJob``    -- submit a task array

Most tasks are plain ``sleep`` calls, so the simulated slots (workers x cpus) do
not contend for real CPU. This keeps the measurement about *scheduling decisions*
rather than throughput, and lets a 16-core box stand in for a small cluster.

The ``P*`` prefill scenarios are the exception: their tasks are short enough that
process spawn dominates, so they really do consume CPU. Those keep the cluster at
or below the host's core count, and their numbers are sensitive to host load in a
way the rest are not.
"""

import dataclasses
from typing import List, Optional, Union

# Default cluster shape: 8 workers x 8 cpus = 64 slots.
WORKER_CPUS = 8
TASKS_PER_JOB = 1000
TASK_SLEEP = 0.5


@dataclasses.dataclass(frozen=True)
class StartWorkers:
    count: int
    cpus: int = WORKER_CPUS
    # `hq worker start --idle-timeout`: the worker stops on its own after this many
    # seconds with nothing to run. Left off by default, so every other scenario keeps
    # its workers for the whole run. Only the `N*` node-hours scenarios set it -- a
    # worker that stops is the *measurement* there, not an incident.
    idle_timeout: Optional[float] = None
    # `hq worker start --time-limit`: the worker stops after this many seconds regardless
    # of load, the way a Slurm/PBS allocation ends at its walltime. With it set, a worker
    # accepts a task only while its *remaining* lifetime still covers the task's
    # `time_request`, which is what makes the `W*` scenarios a one-shot loading window.
    time_limit: Optional[float] = None


@dataclasses.dataclass(frozen=True)
class SubmitJob:
    name: str
    tasks: int = TASKS_PER_JOB
    sleep: float = TASK_SLEEP
    priority: int = 0
    # Tasks with a different cpu request land in a different ResourceRqId, which
    # is what gives each request its own queue on the worker side.
    cpus: int = 1
    # `hq submit --weight` (v0.26+ only): multiplies this request's placement value
    # in the solver objective. Default 1.0; None leaves the flag off entirely so the
    # scenario still runs against v0.25.1.
    weight: Optional[float] = None
    # Overrides the default `sleep <sleep>` program. Used by the prefill scenarios, where the
    # point is a task whose duration is dominated by process spawn rather than by sleeping.
    command: Optional[List[str]] = None
    # Every `straggler_every`-th task sleeps `straggler_factor` times longer.
    straggler_every: int = 0
    straggler_factor: float = 5.0
    # `hq submit --time-request`: the task may only start on a worker with at least this
    # much lifetime left. Against a worker `time_limit` it defines the loading window --
    # a worker can be given such a task only during its first
    # `time_limit - time_request` seconds.
    time_request: Optional[float] = None

    def program(self) -> List[str]:
        if self.command is not None:
            return list(self.command)
        if not self.straggler_every:
            return ["sleep", str(self.sleep)]
        long_sleep = self.sleep * self.straggler_factor
        return [
            "bash",
            "-c",
            f"if [ $((HQ_TASK_ID % {self.straggler_every})) -eq 0 ]; "
            f"then sleep {long_sleep}; else sleep {self.sleep}; fi",
        ]


Event = Union[StartWorkers, SubmitJob]


@dataclasses.dataclass(frozen=True)
class Scenario:
    id: str
    description: str
    purpose: str
    timeline: List["TimedEvent"]

    def total_workers(self) -> int:
        return sum(e.event.count for e in self.timeline if isinstance(e.event, StartWorkers))

    def total_tasks(self) -> int:
        return sum(e.event.tasks for e in self.timeline if isinstance(e.event, SubmitJob))

    def job_names(self) -> List[str]:
        return [e.event.name for e in self.timeline if isinstance(e.event, SubmitJob)]


@dataclasses.dataclass(frozen=True)
class TimedEvent:
    at: float
    event: Event


def _t(at: float, event: Event) -> TimedEvent:
    return TimedEvent(at=at, event=event)


def _interleaved_timeline(priorities: List[int], straggler_every: int = 0) -> List[TimedEvent]:
    """Workers arrive interleaved with submissions -- the recalled setup."""
    p1, p2, p3, p4 = priorities
    return [
        _t(0, StartWorkers(2)),
        _t(2, SubmitJob("job1", priority=p1, straggler_every=straggler_every)),
        _t(6, StartWorkers(3)),
        _t(8, SubmitJob("job2", priority=p2, straggler_every=straggler_every)),
        _t(12, StartWorkers(3)),
        _t(14, SubmitJob("job3", priority=p3, straggler_every=straggler_every)),
        _t(15, SubmitJob("job4", priority=p4, straggler_every=straggler_every)),
    ]


#: `W*` -- worker walltime against task time requests, the shape an HPC allocation actually has.
#: A worker lives `WALLTIME_S` seconds and then stops, as a Slurm/PBS allocation does at its
#: walltime. Tasks request 95 % of that, so a worker can be given work only during the first 5 %
#: of its life; after that it merely drains whatever it was handed. Runtimes are drawn from
#: 30-95 % of the lifetime, so a task always fits in the window its request bought.
WALLTIME_S = 60.0
#: 95 % of `WALLTIME_S`. Also the maximum runtime, so an accepted task always finishes in time.
WALLTIME_REQUEST_S = 57.0
#: 30 % of `WALLTIME_S`, and the span up to 95 %.
WALLTIME_MIN_RUN_S = 18
WALLTIME_RUN_SPAN_S = 40


def _walltime_program() -> List[str]:
    """A per-task runtime drawn from 30-95 % of the worker lifetime.

    Hashed from the task id rather than drawn with `$RANDOM`: repetitions must differ in
    scheduling alone, or an arm could win a repetition merely by drawing shorter tasks.
    """
    return [
        "bash",
        "-c",
        f"sleep $(( {WALLTIME_MIN_RUN_S} + (HQ_TASK_ID * 7919) % {WALLTIME_RUN_SPAN_S} ))",
    ]


def _walltime_timeline(n_workers: int, cpus: int, n_jobs: int, tasks: int) -> List["TimedEvent"]:
    """Workers and submissions both arriving continuously, sorted into one timeline."""
    events = [
        _t(3.0 + 5.0 * i, StartWorkers(1, cpus=cpus, time_limit=WALLTIME_S)) for i in range(n_workers)
    ] + [
        _t(
            1.0 + 12.0 * i,
            SubmitJob(
                f"job{i + 1}",
                tasks=tasks,
                cpus=1,
                sleep=(WALLTIME_MIN_RUN_S + WALLTIME_RUN_SPAN_S / 2),
                time_request=WALLTIME_REQUEST_S,
                command=_walltime_program(),
            ),
        )
        for i in range(n_jobs)
    ]
    return sorted(events, key=lambda e: e.at)


SCENARIOS = {
    # --- Job tails and fairness (comparison with the previous scheduler) -------
    "S3": Scenario(
        id="S3",
        description="interleaved, every 20th task sleeps 5x longer",
        purpose=(
            "Job tails table, 'uneven durations': interleaved worker arrival, every 20th task runs "
            "5x longer."
        ),
        timeline=_interleaved_timeline([0, 0, 0, 0], straggler_every=20),
    ),
    "S4": Scenario(
        id="S4",
        description="late burst: 1 tiny worker, all 4 jobs submitted, then 31 workers join",
        purpose=(
            "Job tails table, 'late worker burst': all four jobs are submitted to one small worker, "
            "then 31 workers join."
        ),
        timeline=[
            _t(0, StartWorkers(1, cpus=2)),
            _t(2, SubmitJob("job1")),
            _t(3, SubmitJob("job2")),
            _t(4, SubmitJob("job3")),
            _t(5, SubmitJob("job4")),
            _t(15, StartWorkers(31, cpus=2)),
        ],
    ),
    "S6": Scenario(
        id="S6",
        description="heterogeneous worker sizes (2 vs 16 cpus), workers join interleaved",
        purpose=(
            "Job tails table, 'heterogeneous': 2- and 16-cpu workers joining interleaved."
        ),
        timeline=[
            _t(0, StartWorkers(2, cpus=2)),
            _t(2, SubmitJob("job1")),
            _t(6, StartWorkers(2, cpus=16)),
            _t(8, SubmitJob("job2")),
            _t(12, StartWorkers(2, cpus=2)),
            _t(13, StartWorkers(2, cpus=16)),
            _t(14, SubmitJob("job3")),
            _t(15, SubmitJob("job4")),
        ],
    ),
    "S7": Scenario(
        id="S7",
        description="two jobs on a heterogeneous cluster (4x16 + 4x2 cpus), all workers up front",
        purpose=(
            "Job tails table, 'two jobs, heterogeneous': 4x16 + 4x2 cpus, all workers up front."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=16)),
            _t(0, StartWorkers(4, cpus=2)),
            _t(2, SubmitJob("job1")),
            _t(3, SubmitJob("job2")),
        ],
    ),
    # --- Narrow and wide jobs ---------------------------------------------------
    "S5": Scenario(
        id="S5",
        description="mixed cpu requests (1 vs 4) on a heterogeneous cluster, all workers up front",
        purpose=(
            "Narrow (1-cpu) and wide (4-cpu) jobs on small and large workers: the workload that "
            "trades makespan for fairness."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=8)),
            _t(0, StartWorkers(4, cpus=2)),
            _t(3, SubmitJob("job1", cpus=1)),
            _t(4, SubmitJob("job2", cpus=4)),
            _t(5, SubmitJob("job3", cpus=1)),
            _t(6, SubmitJob("job4", cpus=4)),
        ],
    ),
    "S5W": Scenario(
        id="S5W",
        description="S5 with the wide (--cpus=4) jobs given --weight=2.0",
        purpose=(
            "S5 with weight 2.0 on the wide jobs, which moves the trade back."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=8)),
            _t(0, StartWorkers(4, cpus=2)),
            _t(3, SubmitJob("job1", cpus=1)),
            _t(4, SubmitJob("job2", cpus=4, weight=2.0)),
            _t(5, SubmitJob("job3", cpus=1)),
            _t(6, SubmitJob("job4", cpus=4, weight=2.0)),
        ],
    ),
    # --- Node-hours, walltimes and compaction ----------------------------------
    "N1": Scenario(
        id="N1",
        description="node-hours: C3's drain with an idle timeout, so unused workers actually stop",
        purpose=(
            "Node-hours: a saturating burst, then a long tail that fits on a couple of workers. "
            "Every worker has an idle timeout, so a worker the scheduler leaves empty stops."
        ),
        timeline=[
            _t(0, StartWorkers(8, cpus=8, idle_timeout=5.0)),
            _t(2, SubmitJob("burst", tasks=400, sleep=0.5)),
            _t(3, SubmitJob("tail", tasks=16, sleep=30.0)),
        ],
    ),
    "W2": Scenario(
        id="W2",
        description="continuous arrival: 20 workers with 60 s walltimes, tasks requesting 95 % of one",
        purpose=(
            "Worker walltimes and time requests: workers with a 60 s walltime arrive every 5 s, and "
            "every task requests 95 % of a walltime, so a worker can be loaded only in the first 3 s "
            "of its life."
        ),
        timeline=_walltime_timeline(n_workers=20, cpus=8, n_jobs=4, tasks=20),
    ),
    "C3": Scenario(
        id="C3",
        description="drain: 8x8 cpus, a saturating burst followed by a long sparse tail",
        purpose=(
            "Compaction figure: a saturating burst followed by a long sparse tail."
        ),
        timeline=[
            _t(0, StartWorkers(8, cpus=8)),
            _t(2, SubmitJob("burst", tasks=400, sleep=0.5)),
            _t(3, SubmitJob("tail", tasks=16, sleep=12.0)),
        ],
    ),
    "C5": Scenario(
        id="C5",
        description="wide task after a drain: 8x8 cpus, burst, 16 long tasks placed as it drains, then one 8-cpu task",
        purpose=(
            "Compaction figure: C3's drain, then a task that needs a whole worker."
        ),
        timeline=[
            _t(0, StartWorkers(8, cpus=8)),
            _t(2, SubmitJob("burst", tasks=400, sleep=0.5)),
            _t(3, SubmitJob("tail", tasks=16, sleep=30.0)),
            _t(12, SubmitJob("big", tasks=1, sleep=2.0, cpus=8)),
        ],
    ),
    # --- Prefilling: swept over HQ_SCHED_PREFILL_MAX on the head arm only -----
    "P0": Scenario(
        id="P0",
        description="near-trivial tasks: 4x4 cpus, 4000 tasks of `true`",
        purpose=(
            "Prefill sweep: near-trivial tasks (`/bin/true`)."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=4)),
            _t(2, SubmitJob("job1", tasks=4000, command=["true"])),
        ],
    ),
    "P1d005": Scenario(
        id="P1d005",
        description="prefill duration curve: 4x4 cpus, 16000 tasks of `sleep 0.005`",
        purpose=(
            "Prefill sweep: 5 ms tasks."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=4)),
            _t(2, SubmitJob("job1", tasks=16000, sleep=0.005)),
        ],
    ),
    "P1d02": Scenario(
        id="P1d02",
        description="prefill duration curve: 4x4 cpus, 8000 tasks of `sleep 0.02`",
        purpose=(
            "Prefill sweep: 20 ms tasks."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=4)),
            _t(2, SubmitJob("job1", tasks=8000, sleep=0.02)),
        ],
    ),
    "P1": Scenario(
        id="P1",
        description="short tasks: 4x4 cpus, 4000 tasks of `sleep 0.05`",
        purpose=(
            "Prefill sweep: 50 ms tasks; also the 'no queue denied' arm of the priority restriction."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=4)),
            _t(2, SubmitJob("job1", tasks=4000, sleep=0.05)),
        ],
    ),
    "P1d2": Scenario(
        id="P1d2",
        description="prefill duration curve: 4x4 cpus, 1600 tasks of `sleep 0.2`",
        purpose=(
            "Prefill sweep: 200 ms tasks."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=4)),
            _t(2, SubmitJob("job1", tasks=1600, sleep=0.2)),
        ],
    ),
    "P3": Scenario(
        id="P3",
        description="two resource requests at different priorities: cpus=2 @prio5, cpus=1 @prio0",
        purpose=(
            "Prefill priority restriction: the cpus=1 queue is below the global top priority, so it "
            "is denied prefilling until the cpus=2 queue drains. Compared against P1."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=4)),
            _t(2, SubmitJob("hi", tasks=2000, sleep=0.05, cpus=2, priority=5)),
            _t(2, SubmitJob("lo", tasks=2000, sleep=0.05, cpus=1, priority=0)),
        ],
    ),
    "P4": Scenario(
        id="P4",
        description="duration imbalance: 4x4 cpus, 2000 tasks, every 20th runs 20x longer",
        purpose=(
            "Prefill sweep, duration imbalance: every 20th task runs 20x longer."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=4)),
            _t(2, SubmitJob("job1", tasks=2000, sleep=0.05, straggler_every=20, straggler_factor=20.0)),
        ],
    ),
    "P5": Scenario(
        id="P5",
        description="retraction churn: a long low-priority job, interrupted by rising priorities",
        purpose=(
            "Prefill under continuous priority churn: a long low-priority job interrupted by "
            "jobs of rising priority, each of which retracts the prefill below it."
        ),
        timeline=[
            _t(0, StartWorkers(4, cpus=4)),
            _t(2, SubmitJob("bulk", tasks=4000, sleep=0.05, priority=0)),
        ]
        + [_t(3.0 + i, SubmitJob(f"churn{i}", tasks=4, sleep=0.05, priority=i + 1)) for i in range(10)],
    ),
}
