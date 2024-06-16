import datetime
import os

import dataclasses
import json
import logging
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

from hyperqueue import Client
from hyperqueue.task.function import PythonEnv
from ..clusterutils import ClusterInfo
from ..clusterutils.cluster_helper import ClusterHelper, StartProcessArgs
from ..clusterutils.node_list import Local
from ..clusterutils.profiler import PROFILER_METADATA_KEY, Profiler
from ..trace.export import export_hq_events_to_chrome
from ..utils import check_file_exists
from ..utils.process import execute_process
from ..utils.timing import wait_until
from . import Environment, EnvironmentDescriptor
from .utils import EnvStateManager


@dataclasses.dataclass(frozen=True)
class HqWorkerResource:
    def format(self) -> str:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class HqSumWorkerResource(HqWorkerResource):
    amount: int

    def format(self) -> str:
        return f"sum({self.amount})"


@dataclasses.dataclass(frozen=True)
class HqIndicesWorkerResource(HqWorkerResource):
    start: int
    end: int

    def format(self) -> str:
        return f"indices({self.start}-{self.end})"


HqWorkerResources = Dict[str, HqWorkerResource]


@dataclasses.dataclass(frozen=True)
class HqWorkerConfig:
    cpus: Optional[int] = None
    node: Optional[int] = None
    resources: HqWorkerResources = dataclasses.field(default_factory=dict)
    overview_interval: datetime.timedelta = datetime.timedelta(seconds=0)


@dataclasses.dataclass
class HqClusterInfo(EnvironmentDescriptor):
    cluster: ClusterInfo
    binary: Path
    workers: List[HqWorkerConfig]
    # Enable debug logging on server and workers
    debug: bool = False
    server_profilers: List[Profiler] = dataclasses.field(default_factory=list)
    worker_profilers: List[Profiler] = dataclasses.field(default_factory=list)
    # Additional information that describes the environment
    environment_params: Dict[str, Any] = dataclasses.field(default_factory=dict)
    # Should events be stored to disk and recorded to JSON?
    # If yes, they will appear at <workdir>/events.json
    generate_event_log: bool = False
    # Whether HyperQueue encryption should be used
    encryption: bool = False

    def __post_init__(self):
        self.binary = self.binary.absolute()
        # Using multiple profilers at once is currently unsupported
        assert len(self.server_profilers) < 2
        assert len(self.worker_profilers) < 2

    def create_environment(self, workdir: Path) -> Environment:
        return HqEnvironment(self, workdir)

    def name(self) -> str:
        return "hq"

    def parameters(self) -> Dict[str, Any]:
        params = dict(worker_count=len(self.workers))
        params.update(self.environment_params)
        return params


def assign_workers(workers: List[HqWorkerConfig], nodes: List[str]) -> Dict[str, List[HqWorkerConfig]]:
    round_robin_node = 0
    used_round_robin = set()

    node_assignments = defaultdict(list)
    for index, worker in enumerate(workers):
        node = worker.node
        if node is not None:
            if not (0 <= node < len(nodes)):
                raise Exception(
                    f"Invalid node assignment. Worker {index} wants to be on node "
                    f"{node}, but there are only {len(nodes)} worker nodes"
                )
        else:
            node = round_robin_node
            round_robin_node = (round_robin_node + 1) % len(nodes)
            if node in used_round_robin:
                logging.warning(f"There are more workers ({len(workers)}) than worker nodes ({len(nodes)})")
            used_round_robin.add(node)
        if node >= len(nodes):
            raise Exception(f"Selected worker node is {node}, but there are only {len(nodes)} worker node(s)")
        node_assignments[nodes[node]].append(worker)
    return dict(node_assignments)


def postprocess_events(binary_path: Path, event_log_path: Path, event_log_json_path: Path, workdir: Path):
    with open(event_log_json_path, "w") as f:
        subprocess.run([str(binary_path), "journal", "export", str(event_log_path)], stdout=f, check=True)
    export_hq_events_to_chrome(event_log_json_path, workdir / "events-chrome.json")


class HqEnvironment(Environment, EnvStateManager):
    def __init__(self, info: HqClusterInfo, workdir: Path):
        EnvStateManager.__init__(self)

        self.info = info
        self.cluster = ClusterHelper(self.info.cluster, workdir=workdir)
        self.binary_path = self.info.binary.absolute()
        check_file_exists(self.binary_path)

        self._workdir = workdir.resolve()
        self.server_dir = self.workdir / "hq"

        self.nodes = self.info.cluster.node_list.resolve()
        assert self.nodes

        worker_nodes = self.nodes if isinstance(self.info.cluster.node_list, Local) else self.nodes[1:]
        if not worker_nodes:
            raise Exception("No worker nodes are available")

        self.worker_count = len(self.info.workers)
        assert self.worker_count > 0
        self.worker_assignment = assign_workers(self.info.workers, worker_nodes)
        logging.debug(f"Worker assignment: {self.worker_assignment}")

        self.submit_id = 0
        self.postprocessing_fns: List[Callable[[], None]] = []

    @property
    def workdir(self) -> Path:
        return self._workdir

    @property
    def server_workdir(self) -> Path:
        return self.workdir / "server"

    def start(self):
        self.state_start()

        # Make sure to kill any previously running HQ binaries
        subprocess.run(["killall", os.path.basename(self.binary_path)], check=False)

        logging.info("Starting HQ server")
        self.start_server()
        self._wait_for_server_start()
        logging.info("HQ server started")

        self.start_workers(self.worker_assignment)
        self.cluster.start_monitoring(self.cluster.active_nodes, observe_processes=True)
        self.cluster.commit()

        self._wait_for_workers(self.worker_count)
        logging.info(f"{self.worker_count} HQ worker(s) connected")
        logging.info(f"Worker info\n{json.loads(self._get_worker_info())}")

    def start_server(self):
        workdir = self.server_workdir

        args = self._shared_args() + ["server", "start"]
        if self.info.generate_event_log:
            event_log_path = workdir / "events.bin"
            event_log_json_path = workdir / "events.json"
            self.postprocessing_fns.append(
                lambda: postprocess_events(self.binary_path, event_log_path, event_log_json_path, workdir)
            )
            args += ["--journal", str(event_log_path)]

        args = StartProcessArgs(
            args=args,
            hostname=self.nodes[0],
            name="server",
            workdir=workdir,
            env=self._shared_envs(),
        )

        apply_profilers(args, self.info.server_profilers, workdir)

        self.cluster.start_processes([args])

    def start_workers(self, worker_assignment: Dict[str, List[HqWorkerConfig]]):
        worker_processes = []

        items = sorted(worker_assignment.items(), key=lambda item: item[0])
        for node, workers in items:
            for worker_index, worker in enumerate(workers):
                worker: HqWorkerConfig = worker

                worker_index = f"worker-{node}-{worker_index}"
                workdir = self.server_dir / worker_index
                args = self._shared_args() + ["worker", "start"]
                if worker.cpus is not None:
                    args += ["--cpus", str(worker.cpus)]
                for name, resource in worker.resources.items():
                    args += ["--resource", f"{name}={resource.format()}"]
                args += [
                    "--overview-interval",
                    f"{int(worker.overview_interval.total_seconds())}s",
                ]
                args = StartProcessArgs(
                    args=args,
                    hostname=node,
                    name=worker_index,
                    workdir=workdir,
                    env=self._shared_envs(),
                )

                apply_profilers(args, self.info.worker_profilers, workdir)
                worker_processes.append(args)

        self.cluster.start_processes(worker_processes)

    def stop(self):
        self.state_stop()

        logging.info("Stopping HQ server and waiting for server and workers to stop")

        # Stop the server
        subprocess.run([*self._shared_args(), "server", "stop"], env=self._shared_envs())
        # Wait for the server and worker to end
        self.cluster.wait_for_process_end(
            lambda p: p.key == "server" or p.key.startswith("worker"),
            duration=datetime.timedelta(seconds=5),
        )
        # Send SIGINT to everything
        self.cluster.stop(use_sigint=True)

        for fn in self.postprocessing_fns:
            fn()

    def submit(self, args: List[str]) -> subprocess.CompletedProcess:
        path = self.server_dir / f"submit-{self.submit_id}"
        stdout = Path(f"{path}.out")
        stderr = Path(f"{path}.err")

        args = self._shared_args() + args
        logging.debug(f"[HQ] Submitting `{' '.join(args)}`")
        result = execute_process(args, stdout=stdout, stderr=stderr, env=self._shared_envs())

        self.submit_id += 1
        return result

    def create_client(self, **kwargs) -> Client:
        if "python_env" not in kwargs:
            kwargs["python_env"] = PythonEnv(prologue=f"source {os.environ['VIRTUAL_ENV']}/bin/activate")
        return Client(self.server_dir, **kwargs)

    def _shared_args(self) -> List[str]:
        return [str(self.binary_path), "--server-dir", str(self.server_dir)]

    def _shared_envs(self) -> Dict[str, str]:
        env = {"RUST_LOG": f"hyperqueue={'DEBUG' if self.info.debug else 'INFO'}"}
        if not self.info.encryption:
            env["HQ_SKIP_AUTHENTICATION"] = "1"
        return env

    def _wait_for_server_start(self):
        wait_until(lambda: (self.server_dir / "hq-current" / "access.json").is_file())

    def _wait_for_workers(self, count: int):
        def get_worker_count():
            output = self._get_worker_info()
            return len(json.loads(output)) == count

        wait_until(lambda: get_worker_count())

    def _get_worker_info(self):
        return subprocess.check_output(
            self._shared_args() + ["--output-mode", "json", "worker", "list"],
            env=self._shared_envs(),
        )


def apply_profilers(args: StartProcessArgs, profilers: List[Profiler], output_dir: Path):
    for profiler in profilers:
        profiler.check_availability()
        result = profiler.profile(args.args, output_dir.absolute())

        metadata = args.metadata.get(PROFILER_METADATA_KEY, {})
        assert result.tag not in metadata
        metadata[result.tag] = str(result.output_path.absolute())

        args.metadata[PROFILER_METADATA_KEY] = metadata
        args.args = result.args
