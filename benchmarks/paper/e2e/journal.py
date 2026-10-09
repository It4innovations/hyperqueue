#!/usr/bin/env python3
"""Shared parsing of an exported HQ journal (``events.ndjson``).

``hq journal export`` emits NDJSON in both v0.25.x and v0.26.x, and the events used
here (``worker-connected``, ``task-started``, ``task-finished``, ``job-created``)
carry the same key names in both, so this is version-neutral. All timestamps are
returned relative to ``server-start``.
"""

import collections
import datetime
import json
from pathlib import Path
from typing import Dict, List, NamedTuple, Tuple


class Task(NamedTuple):
    job: int
    task: int
    worker: int
    start: float
    finish: float


class Run(NamedTuple):
    #: worker id -> number of cpus it declared
    workers: Dict[int, int]
    tasks: List[Task]
    #: job id -> time the job was created (submitted)
    job_created: Dict[int, float]
    #: worker id -> time it connected. A worker cannot be "idle" before it exists,
    #: which matters for any scenario with staggered worker arrival.
    worker_connected: Dict[int, float]
    #: worker id -> (time it stopped, reason). Absent for workers still up at the end
    #: of the run. The reason matters: only `IdleTimeout` is a worker the scheduler
    #: managed to release, anything else is an incident that would otherwise read as
    #: a node-hours saving.
    worker_lost: Dict[int, Tuple[float, str]]
    #: time of the last event in the journal, i.e. how long the run lasted. Workers
    #: still up at the end are charged until this point.
    end: float


def _ts(value: str) -> float:
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _reason(value) -> str:
    """`LostWorkerReason` is an all-unit enum, so serde emits a plain string
    ("IdleTimeout", "Stopped", ...). Tolerate the tagged-object form in case a
    variant ever gains a payload, rather than silently stringifying a dict."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and len(value) == 1:
        return next(iter(value))
    return str(value)


def _cpus(event: dict) -> int:
    """Workers in this benchmark declare cpus as a single Range resource."""
    rng = event["configuration"]["resources"]["resources"][0]["kind"]["Range"]
    return rng["end"] - rng["start"] + 1


def load_run(events: Path) -> Run:
    t0 = None
    workers: Dict[int, int] = {}
    connected: Dict[int, float] = {}
    job_created: Dict[int, float] = {}
    started: Dict[Tuple[int, int], Tuple[int, float]] = {}
    tasks: List[Task] = []
    lost: Dict[int, Tuple[float, str]] = {}
    last = None

    for line in events.open():
        record = json.loads(line)
        event = record["event"]
        time = _ts(record["time"])
        kind = event["type"]
        last = time if last is None else max(last, time)
        if kind == "server-start":
            t0 = time
        elif kind == "worker-connected":
            workers[event["id"]] = _cpus(event)
            connected[event["id"]] = time
        elif kind == "worker-lost":
            lost[event["id"]] = (time, _reason(event["reason"]))
        elif kind == "job-created":
            job_created[event["job"]] = time
        elif kind == "task-started":
            started[(event["job"], event["task"])] = (event["worker"], time)
        elif kind == "task-finished":
            worker, start = started[(event["job"], event["task"])]
            tasks.append(Task(event["job"], event["task"], worker, start, time))

    if t0 is None:
        raise ValueError(f"{events}: no server-start event")
    return Run(
        workers=workers,
        tasks=[t._replace(start=t.start - t0, finish=t.finish - t0) for t in tasks],
        job_created={j: t - t0 for j, t in job_created.items()},
        worker_connected={w: t - t0 for w, t in connected.items()},
        worker_lost={w: (t - t0, reason) for w, (t, reason) in lost.items()},
        end=(last - t0) if last is not None else 0.0,
    )


def iter_runs(results: Path, scenarios=None, versions=None):
    """Yield ``(scenario, version, rep, path)`` for every run under ``results``."""
    for events in sorted(results.glob("*/*/*/events.ndjson")):
        rep_dir = events.parent
        version = rep_dir.parent.parent.name
        scenario = rep_dir.parent.name
        if scenarios and scenario not in scenarios:
            continue
        if versions and version not in versions:
            continue
        yield scenario, version, rep_dir.name, events


def load_meta(events: Path) -> dict:
    """Read the ``meta.json`` written next to an ``events.ndjson``."""
    return json.loads((events.parent / "meta.json").read_text())


def cpus_per_job(meta: dict) -> Dict[int, int]:
    """job id -> cpus requested per task, from the harness metadata.

    The journal records which worker ran a task but not what it requested, so the
    resource demand has to come from the scenario as recorded at run time.
    """
    return {job["job_id"]: job.get("cpus", 1) for job in meta["jobs"]}


def occupancy_timeline(run: Run, job_cpus: Dict[int, int]):
    """-> sorted list of ``(time, {worker_id: cpus_in_use})`` change points.

    Reconstructs, for every instant, how many cpus each worker had in use. A task
    holds ``job_cpus[job]`` cpus on its worker between its start and its finish.
    """
    deltas = collections.defaultdict(collections.Counter)
    for t in run.tasks:
        cpus = job_cpus.get(t.job, 1)
        deltas[t.start][t.worker] += cpus
        deltas[t.finish][t.worker] -= cpus

    in_use: collections.Counter = collections.Counter()
    timeline = []
    for time in sorted(deltas):
        in_use.update(deltas[time])
        # Counter.update keeps zero/negative entries; drop the idle workers.
        timeline.append((time, {w: c for w, c in in_use.items() if c > 0}))
    return timeline
