import datetime
import logging
from pathlib import Path
from typing import Dict, Any, List

from hyperqueue import Client, Job
from hyperqueue.job import SubmittedJob
from src.benchmark.database import Database
from src.benchmark.identifier import BenchmarkDescriptor
from src.build.hq import BuildConfig, iterate_binaries
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local
from src.environment.hq import HqClusterInfo, HqWorkerConfig, HqEnvironment
from src.postprocessing.report import ClusterReport
from src.submit.slurm import SlurmOptions, run_in_slurm
from src.utils import activate_cwd, ensure_directory
from src.utils.benchmark import run_benchmarks_with_postprocessing, DEFAULT_DATA_JSON, has_work_left
from src.utils.timing import Timings
from src.workloads import Workload
from src.workloads.utils import create_result
from src.workloads.workload import WorkloadExecutionResult

LIGEN_ROOT = Path("/mnt/proj2/dd-21-9/beranekj/awh-hq")
BENCH_WORKDIR = Path("benchmark/ligen")


class LigenWorkload(Workload):
    def __init__(self, smi_path: Path, max_molecules: int, screening_threads: int):
        self.smi_path = smi_path
        self.max_molecules = max_molecules
        self.screening_threads = min(max_molecules, screening_threads)
        if self.screening_threads != screening_threads:
            logging.warning(f"Setting screening threads to {self.max_molecules}, because there won't be more work")

    def name(self) -> str:
        return "ligen-vscreen"

    def parameters(self) -> Dict[str, Any]:
        return {
            "smi": str(self.smi_path),
            "molecules-per-task": self.max_molecules,
            "screening-threads": self.screening_threads,
        }

    def execute(self, env: HqEnvironment) -> WorkloadExecutionResult:
        timer = Timings()
        with activate_cwd(env.workdir):
            client = env.create_client()
            submitted = self.submit_ligen_benchmark(env, client)
            with timer.time():
                client.wait_for_jobs([submitted])
        return create_result(timer.duration())

    def submit_ligen_benchmark(self, env: HqEnvironment, client: Client) -> SubmittedJob:
        from ligate.ligen.common import LigenTaskContext
        from ligate.ligen.expansion import (
            create_configs_from_smi,
            submit_expansion,
        )
        from ligate.ligen.virtual_screening import ScreeningConfig, submit_screening

        workdir = ensure_directory(env.workdir / "ligen-work")
        inputs = ensure_directory(workdir / "expansion" / "inputs")
        outputs = ensure_directory(workdir / "expansion" / "outputs")

        DATA_DIR = LIGEN_ROOT / "ligenApptainer" / "example"
        CONTAINER_PATH = LIGEN_ROOT / "ligen.sif"
        ctx = LigenTaskContext(workdir=workdir, container_path=Path(CONTAINER_PATH).absolute())

        expansion_configs = create_configs_from_smi(
            input_smi=self.smi_path,
            workdir_inputs=inputs,
            workdir_outputs=outputs,
            max_molecules=self.max_molecules,
        )

        job = Job(workdir, default_env=dict(HQ_PYLOG="DEBUG"))
        expand_tasks = []
        for config in expansion_configs:
            expand_tasks.append(submit_expansion(ctx, config, job))

        for task in expand_tasks:
            submit_screening(
                ctx,
                ScreeningConfig(
                    input_mol2=DATA_DIR / "crystal.mol2",
                    input_pdb=DATA_DIR / "protein.pdb",
                    output_path=Path(f"screening-{task.config.id}.csv"),
                    input_protein_name="1CVU",
                    ligand_expansion=task,
                    cores=self.screening_threads,
                ),
                job,
            )

        return client.submit(job)


def analyze_results(data_path: Path):
    db = Database(data_path)
    for key, row in db.data.items():
        workdir = row.benchmark_metadata["workdir"]
        workload = row.workload
        params = row.workload_params
        duration = row.duration

        cluster_report = ClusterReport.load(Path(workdir))
        for node, records in cluster_report.monitoring.items():
            processes = node.processes
            worker_processes = tuple(proc.pid for proc in processes if proc.key.startswith("worker"))
            if len(worker_processes) > 0:
                for record in records:
                    for pid, process_resources in record.processes.items():
                        pid = int(pid)
                        if pid in worker_processes:
                            print(record.timestamp, process_resources)

        print(workload, params, duration, workdir)


def build_descriptions() -> List[BenchmarkDescriptor]:
    hq_path = list(iterate_binaries([BuildConfig()]))[0].binary_path

    worker_count = 1
    worker_threads = 32

    env = HqClusterInfo(
        cluster=ClusterInfo(monitor_nodes=True, node_list=Local()),
        environment_params=dict(worker_count=1),
        workers=[HqWorkerConfig(cpus=worker_threads)],
        binary=hq_path,
        worker_profilers=[],
    )

    descriptions = []

    input_smi = LIGEN_ROOT / "ligenApptainer/dataset/ligands.smi"
    thread_count = [1, 8]

    for threads in thread_count:
        workload = LigenWorkload(smi_path=input_smi, max_molecules=100, screening_threads=threads)
        descriptions.append(BenchmarkDescriptor(env_descriptor=env, workload=workload))

    return descriptions


def run():
    # shutil.rmtree("benchmark", ignore_errors=True)

    descriptions = build_descriptions()
    if has_work_left(BENCH_WORKDIR, descriptions):

        def compute():
            run_benchmarks_with_postprocessing(BENCH_WORKDIR, descriptions)
            analyze_results(BENCH_WORKDIR / DEFAULT_DATA_JSON)

        run_in_slurm(
            SlurmOptions(
                name="slurm-auto-submit",
                queue="qcpu_exp",
                project="DD-21-9",
                walltime=datetime.timedelta(minutes=1),
                init_script=Path("/mnt/proj2/dd-21-9/beranekj/modules.sh"),
                workdir=Path("slurm").absolute(),
            ),
            compute,
        )
    else:
        print("Everything has been computed")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s:%(asctime)s.%(msecs)03d:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run()
