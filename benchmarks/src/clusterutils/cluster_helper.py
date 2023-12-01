import datetime

import dataclasses
import functools
import logging
import time
from multiprocessing import Pool
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

import psutil
from cluster.cluster import (
    Cluster,
    Node,
    Process,
    ProcessInfo,
    kill_process,
    start_process,
)

from ..utils import get_pyenv_from_env
from . import ClusterInfo

CLUSTER_FILENAME = "cluster.json"
CURRENT_DIR = Path(__file__).absolute().parent
MONITOR_SCRIPT_PATH = CURRENT_DIR.parent / "monitoring" / "monitor_script.py"
assert MONITOR_SCRIPT_PATH.is_file()


@dataclasses.dataclass
class StartProcessArgs:
    args: List[str]
    hostname: str
    name: str
    workdir: Optional[Path] = None
    env: Dict[str, str] = dataclasses.field(default_factory=dict)
    init_cmd: List[str] = dataclasses.field(default_factory=list)
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def __repr__(self):
        return f"Process `{self.name}/{self.hostname}`: `{' '.join(self.args)}` at `{self.workdir}`"


class ClusterHelper:
    def __init__(self, cluster_info: ClusterInfo, workdir: Path):
        self.cluster_info = cluster_info
        self.workdir = workdir
        self.workdir.mkdir(exist_ok=True, parents=True)

        self.cluster = Cluster(str(self.workdir))

    @property
    def active_nodes(self) -> List[str]:
        return list(self.cluster.nodes.keys())

    @property
    def processes(self) -> List[ProcessInfo]:
        processes = []
        for node in self.cluster.nodes.values():
            processes += node.processes
        return processes

    def wait_for_process_end(
        self, filter_fn: Callable[[ProcessInfo], bool], duration: datetime.timedelta = datetime.timedelta(seconds=5)
    ):
        """
        Wait until processed that pass through the given `filter_fn` are stopped.
        :param filter_fn: Filter function to select a process.
        :param duration: How long to wait (for all processes together) at most.
        """
        start = time.time()
        for process_info in self.processes:
            if filter_fn(process_info):
                try:
                    process = psutil.Process(process_info.pid)
                except psutil.NoSuchProcess:
                    logging.warning(f"Process {process_info.pid} has already stopped")
                    continue

                while process.is_running():
                    if time.time() - start < duration.total_seconds():
                        time.sleep(0.1)
                    else:
                        return

    def commit(self):
        with open(self.workdir / CLUSTER_FILENAME, "w") as f:
            self.cluster.serialize(f)

    def stop(self, use_sigint=False):
        start = time.time()

        fn = functools.partial(kill_fn, use_sigint)
        self.cluster.kill(fn)
        logging.debug(f"Cluster killed in {time.time() - start} seconds")

    def start_processes(self, processes: List[StartProcessArgs]):
        def prepare_workdir(workdir: Path) -> Path:
            workdir = workdir if workdir else self.workdir
            workdir.mkdir(parents=True, exist_ok=True)
            return workdir.absolute()

        pool_args = [dataclasses.replace(args, workdir=prepare_workdir(args.workdir)) for args in processes]

        logging.debug(f"Starting cluster processes: {pool_args}")

        for process in pool_args:
            logging.debug(f"Command: {' '.join(process.args)}")
        spawned = []
        if len(pool_args) == 1:
            spawned.append(start_process_pool(pool_args[0]))
        else:
            with Pool() as pool:
                for res in pool.map(start_process_pool, pool_args):
                    spawned.append(res)

        for process, args in zip(spawned, pool_args):
            self.cluster.add(process=process, key=args.name, **args.metadata)

    def start_monitoring(self, nodes: List[str], observe_processes=False):
        if not self.cluster_info.monitor_nodes:
            return

        init_cmd = []
        pyenv = get_pyenv_from_env()
        if pyenv:
            init_cmd += [f"source {pyenv}/bin/activate"]
        else:
            logging.warning("No Python virtualenv detected. Monitoring will probably not work.")

        nodes = sorted(set(nodes))
        workdir = self.workdir / "monitoring"
        processes = []
        for node in nodes:
            args = [
                "python",
                str(MONITOR_SCRIPT_PATH),
                str(node_monitoring_trace(self.workdir, node)),
            ]
            if observe_processes:
                node_processes = self.cluster.get_processes(hostname=node)
                pids = [str(process.pid) for (_, process) in node_processes]
                if pids:
                    args += ["--observe-pids", ",".join(pids)]
            process = StartProcessArgs(
                args=args,
                hostname=node,
                name="monitor",
                workdir=workdir,
                init_cmd=init_cmd,
            )
            processes.append(process)
        self.start_processes(processes)


def node_monitoring_trace(directory: Path, hostname: str) -> Path:
    return directory / "monitoring" / f"monitoring-{hostname}.trace"


def kill_fn(scheduler_sigint: bool, node: Node, process: ProcessInfo):
    signal = "TERM"
    if scheduler_sigint or "monitor" in process.key:
        signal = "INT"

    if not kill_process(node.hostname, process.pgid, signal=signal):
        logging.warning(f"Error when attempting to kill {process} on {node.hostname}")


def start_process_pool(args: StartProcessArgs) -> Process:
    return start_process(
        args.args,
        hostname=args.hostname,
        workdir=str(args.workdir),
        name=args.name,
        env=args.env,
        init_cmd=args.init_cmd,
    )
