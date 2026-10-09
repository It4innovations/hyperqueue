#!/usr/bin/env python3
"""Run the scheduler-fairness benchmark for one or more (version, scenario, rep).

Each run starts a fresh HQ server with a journal, plays the scenario timeline
(worker starts and job submissions at fixed offsets), waits for every job to
finish, and exports the journal as NDJSON. All metrics are derived later from
that journal by ``analyze.py``.
"""

import argparse
import dataclasses
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from fetch_binaries import ALL_ARMS, ARMS, binary_for, head_sha
from scenarios import SCENARIOS, Scenario, StartWorkers, SubmitJob

HERE = Path(__file__).resolve().parent
DEFAULT_RESULTS = HERE / "results"

SERVER_READY_TIMEOUT = 30.0
JOB_WAIT_TIMEOUT = 900.0


class RunFailed(Exception):
    pass


@dataclasses.dataclass
class Process:
    name: str
    popen: subprocess.Popen


class Cluster:
    """Owns the server and worker processes for a single run."""

    def __init__(
        self,
        binary: Path,
        server_dir: Path,
        log_dir: Path,
        env: Optional[Dict[str, str]] = None,
    ):
        self.binary = binary
        self.server_dir = server_dir
        self.log_dir = log_dir
        self.processes: List[Process] = []
        self.n_workers = 0
        # Extra environment for spawned processes. The scheduler's HQ_SCHED_* knobs are read by
        # `SchedulerConfig::from_env()` at server startup, so they have to be here rather than
        # passed as flags.
        self.env = {**os.environ, **(env or {})}

    # -- process helpers -------------------------------------------------

    def _spawn(self, name: str, args: List[str]) -> Process:
        logfile = self.log_dir / f"{name}.log"
        with open(logfile, "w") as out:
            popen = subprocess.Popen(
                [str(self.binary), f"--server-dir={self.server_dir}"] + args,
                stdout=out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=self.env,
            )
        proc = Process(name=name, popen=popen)
        self.processes.append(proc)
        return proc

    def _run(self, args: List[str], check: bool = True, timeout: float = 120.0):
        return subprocess.run(
            [str(self.binary), f"--server-dir={self.server_dir}"] + args,
            capture_output=True,
            text=True,
            check=check,
            env=self.env,
            timeout=timeout,
        )

    # -- lifecycle -------------------------------------------------------

    def start_server(self, journal: Path) -> None:
        self._spawn(
            "server",
            ["server", "start", f"--journal={journal}", "--journal-flush-period=1s"],
        )
        deadline = time.time() + SERVER_READY_TIMEOUT
        while time.time() < deadline:
            result = self._run(["server", "info"], check=False, timeout=10)
            if result.returncode == 0:
                return
            time.sleep(0.2)
        raise RunFailed("server did not become ready in time")

    def start_workers(
        self,
        count: int,
        cpus: int,
        idle_timeout: Optional[float] = None,
        time_limit: Optional[float] = None,
    ) -> None:
        for _ in range(count):
            self.n_workers += 1
            args = [
                "worker",
                "start",
                f"--cpus={cpus}",
                "--detect-resources=none",
                "--overview-interval=0",
            ]
            if idle_timeout is not None:
                args.append(f"--idle-timeout={idle_timeout}s")
            if time_limit is not None:
                # The clock starts when the process starts, not when it connects, so the
                # usable loading window is shorter than `time_limit - time_request` by the
                # worker's start-up latency. Scenarios must leave room for that.
                args.append(f"--time-limit={time_limit}s")
            self._spawn(f"worker{self.n_workers}", args)

    def wait_for_workers(self, expected: int, timeout: float = 60.0) -> None:
        # `--all` counts workers that have already stopped, which matters once a scenario
        # sets an idle timeout: `expected` is cumulative over the timeline, so a worker
        # that timed out earlier must still count towards it or this would never be
        # satisfied. Without an idle timeout the two listings are identical.
        deadline = time.time() + timeout
        connected = 0
        while time.time() < deadline:
            result = self._run(["--output-mode=json", "worker", "list", "--all"], check=False)
            if result.returncode == 0:
                try:
                    connected = len(json.loads(result.stdout))
                except json.JSONDecodeError:
                    connected = 0
                if connected >= expected:
                    return
            time.sleep(0.2)
        raise RunFailed(f"only {connected}/{expected} workers connected in time")

    def submit(self, job: SubmitJob) -> int:
        args = [
            "submit",
            f"--name={job.name}",
            f"--array=0-{job.tasks - 1}",
            f"--priority={job.priority}",
            f"--cpus={job.cpus}",
            "--stdout=none",
            "--stderr=none",
        ]
        if job.weight is not None:
            # v0.26+ only; scenarios that need to run against v0.25.1 leave it unset.
            args.append(f"--weight={job.weight}")
        if job.time_request is not None:
            args.append(f"--time-request={job.time_request}s")
        args += ["--"] + job.program()
        result = self._run(args)
        # "Job submitted successfully, job ID: 3"
        return int(result.stdout.strip().rsplit(":", 1)[1])

    def wait_for_jobs(self, timeout: float = JOB_WAIT_TIMEOUT) -> bool:
        """Block until every job finishes, or until `timeout`. True if they all finished.

        `hq job wait` exit codes changed meaning in v0.25.0, so we only use it to block, and
        verify completion from the journal afterwards. A timeout is **not** a failed run: the
        `W*` scenarios can legitimately strand work, and the journal showing how much is the
        measurement. `check=False` does not cover this -- `subprocess` raises `TimeoutExpired`
        regardless -- so it must be caught here or the run aborts before exporting its journal.
        """
        try:
            self._run(["job", "wait", "all"], check=False, timeout=timeout)
            return True
        except subprocess.TimeoutExpired:
            print(f"  jobs did not all finish within {timeout:.0f}s; exporting anyway")
            return False

    def stop(self) -> None:
        self._run(["journal", "flush"], check=False, timeout=60)
        self._run(["server", "stop"], check=False, timeout=60)
        deadline = time.time() + 15
        for proc in self.processes:
            remaining = max(0.5, deadline - time.time())
            try:
                proc.popen.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                pass
        for proc in self.processes:
            if proc.popen.poll() is None:
                try:
                    os.killpg(os.getpgid(proc.popen.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass

    def export_journal(self, journal: Path, target: Path) -> None:
        with open(target, "w") as out:
            subprocess.run(
                [str(self.binary), "journal", "export", str(journal)],
                stdout=out,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=300,
            )


def check_no_panic(log_dir: Path) -> None:
    """Raise if any process in this run died of a Rust panic.

    Without this a server crash is silent: `wait_for_jobs` runs `hq job wait all` with
    `check=False` (the exit code is unreliable across versions), so a dead server simply returns,
    the journal is exported truncated, and the run reports success. That is exactly how the
    `solver.rs` gap crash hid inside an otherwise green 144-run sweep -- only the
    `tasks_finished == expected` check in the *analysis* scripts caught it, long afterwards.
    """
    for log in sorted(log_dir.glob("*.log")):
        for line in log.open(errors="replace"):
            if "panicked at" in line:
                raise RunFailed(f"{log.name}: {line.strip()}")


def run_one(
    version: str,
    scenario: Scenario,
    rep: int,
    results: Path,
    env: Optional[Dict[str, str]] = None,
    tag: str = "",
    job_wait_timeout: float = JOB_WAIT_TIMEOUT,
) -> Path:
    binary = binary_for(version)
    if not binary.exists():
        raise RunFailed(f"{binary} not found -- run fetch_binaries.py first")

    # `tag` distinguishes the cells of an --sweep-env sweep. It is appended to the rep
    # directory rather than inserted as a path component, because `journal.iter_runs` globs
    # at a fixed `version/scenario/rep` depth.
    out_dir = results / version / scenario.id / f"rep{rep}{tag}"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    server_dir = out_dir / "server-dir"
    journal = out_dir / "journal.bin"

    cluster = Cluster(binary, server_dir, out_dir, env)
    meta = {
        "version": version,
        "scenario": scenario.id,
        "description": scenario.description,
        "rep": rep,
        "host": platform.node(),
        "cpu_count": os.cpu_count(),
        "jobs": [],
        "expected_tasks": scenario.total_tasks(),
        "expected_workers": scenario.total_workers(),
        # Recorded so a results directory is self-describing rather than depending on whatever
        # was in the environment when it was produced.
        "env": dict(env or {}),
    }
    if version == "head":
        # Which build produced this, so a results dir does not rely on the mtime of bin/hq-head.
        meta["commit"] = head_sha()

    print(f"[{version}/{scenario.id}/rep{rep}{tag}] starting")
    try:
        cluster.start_server(journal)
        t0 = time.time()
        meta["t0_wallclock"] = t0

        workers_started = 0
        for item in scenario.timeline:
            delay = item.at - (time.time() - t0)
            if delay > 0:
                time.sleep(delay)
            if isinstance(item.event, StartWorkers):
                cluster.start_workers(
                    item.event.count,
                    item.event.cpus,
                    item.event.idle_timeout,
                    item.event.time_limit,
                )
                workers_started += item.event.count
                cluster.wait_for_workers(workers_started)
                print(f"  t={time.time() - t0:6.1f}s  {workers_started} workers up")
            elif isinstance(item.event, SubmitJob):
                job_id = cluster.submit(item.event)
                meta["jobs"].append(
                    {
                        "job_id": job_id,
                        "name": item.event.name,
                        "tasks": item.event.tasks,
                        "priority": item.event.priority,
                        # The journal records where a task ran but not what it
                        # requested, so the resource demand has to be recorded here.
                        "cpus": item.event.cpus,
                        "sleep": item.event.sleep,
                        "submitted_at": time.time() - t0,
                    }
                )
                print(
                    f"  t={time.time() - t0:6.1f}s  submitted {item.event.name} "
                    f"(id={job_id}, prio={item.event.priority})"
                )

        meta["all_jobs_finished"] = cluster.wait_for_jobs(timeout=job_wait_timeout)
        meta["wall_time"] = time.time() - t0
        print(f"  t={meta['wall_time']:6.1f}s  all jobs finished")
    finally:
        cluster.stop()

    # Export before checking for a panic, so a failed run still leaves the full evidence behind:
    # the partial journal is what shows *how far* the run got, which is how the `solver.rs` crash
    # was diagnosed. A panic can also break the export itself, so report it in preference to the
    # export error, which is only a symptom.
    try:
        cluster.export_journal(journal, out_dir / "events.ndjson")
    except Exception:
        check_no_panic(out_dir)
        raise
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    check_no_panic(out_dir)

    # Only on success: the journal is large, and on failure it is evidence.
    journal.unlink(missing_ok=True)
    shutil.rmtree(server_dir, ignore_errors=True)
    return out_dir


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", action="append", dest="scenarios", help="scenario id")
    parser.add_argument("--all", action="store_true", help="run every scenario")
    parser.add_argument("--version", action="append", dest="versions", help="hq version")
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument(
        "--job-wait-timeout",
        type=float,
        default=JOB_WAIT_TIMEOUT,
        help=(
            "seconds to wait for the jobs to finish before giving up on the run. Lower it for "
            "scenarios that can legitimately fail to complete -- the journal is exported either "
            "way, so an incomplete run is evidence rather than a lost slot. Default 900."
        ),
    )
    parser.add_argument(
        "--env",
        action="append",
        dest="env",
        metavar="KEY=VAL",
        help="extra environment for spawned hq processes (repeatable), e.g. HQ_SCHED_PREFILL_MAX=0",
    )
    parser.add_argument(
        "--sweep-env",
        metavar="KEY=V1,V2,...",
        help=(
            "sweep one environment variable over several values *within this invocation*, "
            "e.g. HQ_SCHED_PREFILL_MAX=0,4,16,40. The values become the innermost loop, so "
            "every cell of the sweep shares the same power and thermal conditions -- the only "
            "way wall-clock numbers from different cells are comparable on a host with "
            "scaling_governor=powersave. Each cell lands in rep<N>-<key-suffix><value>."
        ),
    )
    args = parser.parse_args()

    if args.all:
        scenario_ids = list(SCENARIOS)
    elif args.scenarios:
        scenario_ids = args.scenarios
    else:
        parser.error("pass --scenario ID (repeatable) or --all")

    unknown = [s for s in scenario_ids if s not in SCENARIOS]
    if unknown:
        parser.error(f"unknown scenario(s): {', '.join(unknown)}")

    env = {}
    for item in args.env or []:
        key, _, value = item.partition("=")
        if not _:
            parser.error(f"--env expects KEY=VAL, got {item!r}")
        env[key] = value

    # (tag, extra env) for each cell of the innermost loop. Without --sweep-env there is a
    # single, untagged cell, so the loop below is exactly the old one.
    cells = [("", {})]
    if args.sweep_env:
        key, _, values = args.sweep_env.partition("=")
        if not _ or not values:
            parser.error(f"--sweep-env expects KEY=V1,V2,..., got {args.sweep_env!r}")
        if key in env:
            parser.error(f"{key} is set by both --env and --sweep-env")
        # A short tag keeps directory names readable; the authoritative value is in meta.json.
        prefix = "".join(word[0] for word in key.lower().replace("hq_sched_", "").split("_"))
        cells = [(f"-{prefix}{v}", {key: v}) for v in values.split(",")]

    versions = args.versions or ARMS
    unknown_versions = [v for v in versions if v not in ALL_ARMS]
    if unknown_versions:
        parser.error(f"unknown arm(s): {', '.join(unknown_versions)} (known: {', '.join(ALL_ARMS)})")

    failures = []
    for rep in range(1, args.reps + 1):
        for scenario_id in scenario_ids:
            for version in versions:
                for tag, cell_env in cells:
                    label = f"{version}/{scenario_id}/rep{rep}{tag}"
                    try:
                        run_one(
                            version,
                            SCENARIOS[scenario_id],
                            rep,
                            args.results,
                            {**env, **cell_env},
                            tag=tag,
                            job_wait_timeout=args.job_wait_timeout,
                        )
                    except Exception as e:  # noqa: BLE001 - keep the matrix going
                        print(f"FAILED {label}: {e}", file=sys.stderr)
                        failures.append((label, str(e)))

    if failures:
        print(f"\n{len(failures)} run(s) failed:", file=sys.stderr)
        for label, error in failures:
            print(f"  {label}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
