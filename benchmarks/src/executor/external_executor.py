import json
import os
import pickle
import shutil
import subprocess
import threading
import traceback
from pathlib import Path
from queue import Queue
from typing import Optional

from .. import ROOT_DIR
from ..benchmark.identifier import BenchmarkDescriptor
from ..benchmark.result import BenchmarkResult, Failure, Timeout
from ..utils.timing import TimeoutException, with_timeout
from .executor import BenchmarkContext, BenchmarkExecutor
from .serialization import SerializedBenchmark, deserialize_result

CURRENT_DIR = Path(__file__).absolute().parent
EXECUTE_SCRIPT_PATH = CURRENT_DIR / "executor_script.py"
assert EXECUTE_SCRIPT_PATH.is_file()


class ExternalBenchmarkExecutor(BenchmarkExecutor):
    """Executes benchmarks in an external process"""

    def __init__(self, init_script: Optional[Path] = None):
        self.init_script = init_script.resolve()
        raise NotImplementedError()

    def execute(self, benchmark: BenchmarkDescriptor, ctx: BenchmarkContext) -> BenchmarkResult:
        return execute_benchmark_in_external_process(benchmark, ctx, self.init_script)


def serialize_benchmark(descriptor: BenchmarkDescriptor, ctx: BenchmarkContext) -> bytes:
    serialized_benchmark = SerializedBenchmark(descriptor=descriptor, ctx=ctx, cwd=Path(os.getcwd()).absolute())
    return pickle.dumps(serialized_benchmark)


def execute_benchmark_in_external_process(
    descriptor: BenchmarkDescriptor,
    ctx: BenchmarkContext,
    init_script: Optional[Path] = None,
) -> BenchmarkResult:
    serialized_benchmark = serialize_benchmark(descriptor, ctx)

    module_path = str(EXECUTE_SCRIPT_PATH.relative_to(ROOT_DIR)).replace("/", ".")[:-3]
    pipe_path = create_fifo(ctx)

    command = ""
    if init_script is not None:
        command += f"source {str(init_script)} || exit 1\n"
    command += f"python -m {module_path} {pipe_path}"

    try:
        return with_timeout(
            lambda: run_in_process(
                command,
                pipe_path=pipe_path,
                serialized_benchmark=serialized_benchmark,
                ctx=ctx,
            ),
            timeout_s=ctx.timeout_s,
        )
    except TimeoutException:
        return Timeout(ctx.timeout_s)
    except BaseException:
        return Failure(traceback.format_exc())


def create_fifo(ctx: BenchmarkContext) -> Path:
    fifo_dir = ctx.workdir / "external-process"
    if fifo_dir.exists():
        shutil.rmtree(fifo_dir, ignore_errors=True)
    fifo_dir.mkdir(parents=True, exist_ok=False)

    path = fifo_dir / "process-pipe"
    if path.exists():
        os.unlink(path)
    os.mkfifo(path)
    return path


def run_in_process(
    command: str, pipe_path: Path, serialized_benchmark: bytes, ctx: BenchmarkContext
) -> BenchmarkResult:
    """
    Run the given `command` in an external process and read the output from `pipe_path`.
    Stdout and stderr of the process will be stored in the directory of `pipe_path`.
    """
    pipe_dir = pipe_path.parent

    stdin_file = pipe_dir / "stdin"
    stdout_file = pipe_dir / "stdout"
    stderr_file = pipe_dir / "stderr"

    with open(stdin_file, "wb") as f:
        f.write(serialized_benchmark)

    with open(stdin_file, "rb") as stdin:
        with open(stdout_file, "wb") as stdout:
            with open(stderr_file, "wb") as stderr:
                process = subprocess.Popen(
                    ["/bin/bash", "-c", command],
                    stdin=stdin,
                    stdout=stdout,
                    stderr=stderr,
                    cwd=ROOT_DIR,
                )

                def create_error_message(returncode: int) -> str:
                    with open(stdout_file) as stdout:
                        with open(stderr_file) as stderr:
                            return (
                                "Benchmark launch in external process ended with exception.\n"
                                f"Stdout: {stdout.read()}\n"
                                f"Stderr: {stderr.read()}\n"
                                f"Exit code: {returncode}"
                            )

                output_queue = Queue()

                def read_output():
                    with open(pipe_path) as file:
                        data = json.loads(file.read())
                        output_queue.put(data)

                """
                Spawn a thread so that we can read the output and wait for the process concurrently.
                With this approach we can immediately recognize that the process has failed and at
                the same time we do not block on the pipe, because it is read from another thread.
                """
                thread = threading.Thread(target=read_output)
                thread.start()

                process.wait(timeout=ctx.timeout_s)
                if process.returncode != 0:
                    raise Exception(create_error_message(process.returncode))

                try:
                    data = output_queue.get(timeout=ctx.timeout_s)
                    thread.join()
                    return deserialize_result(data)
                except BaseException as e:
                    raise Exception(create_error_message(process.returncode)) from e
