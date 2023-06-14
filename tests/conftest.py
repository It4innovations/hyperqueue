import contextlib
import json
import os
import signal
import subprocess
import time
from typing import Iterable, List, Optional, Tuple

import pytest

from .utils import parse_tables
from .utils.mock import ProgramMock
from .utils.wait import wait_until

PYTEST_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PYTEST_DIR)


def get_hq_binary(debug=True):
    directory = "debug" if debug else "release"
    return os.path.join(ROOT_DIR, "target", directory, "hq")


RUNNING_IN_CI = "CI" in os.environ


class Env:
    def __init__(self, work_path):
        self.processes = []
        self.work_path = work_path

    def start_process(self, name, args, env=None, catch_io=True, cwd=None):
        cwd = str(cwd or self.work_path)
        logfile = (self.work_path / name).with_suffix(".out")
        print(f"Starting process {name} with logfile {logfile}")
        if catch_io:
            with open(logfile, "w") as out:
                p = subprocess.Popen(
                    args,
                    preexec_fn=os.setsid,
                    stdout=out,
                    stderr=subprocess.STDOUT,
                    cwd=cwd,
                    env=env,
                )
        else:
            p = subprocess.Popen(args, cwd=cwd, env=env)
        self.processes.append((name, p))
        return p

    def check_process_exited(self, process: subprocess.Popen, expected_code=0):
        def is_process_alive():
            for n, p in self.processes:
                if p is process:
                    if process.poll() is None:
                        return True
                    if expected_code == "error":
                        assert process.returncode != 0
                    elif expected_code is not None:
                        assert process.returncode == expected_code

                    self.processes = [
                        (n, p) for (n, p) in self.processes if p is not process
                    ]
                    return False
            raise Exception(f"Process with pid {process.pid} not found")

        wait_until(lambda: not is_process_alive())

    def check_running_processes(self):
        """Checks that everything is still running"""
        for name, process in self.processes:
            if process.poll() is not None:
                raise Exception(
                    "Process {0} crashed (log in {1}/{0}.out)".format(
                        name, self.work_path
                    )
                )

    def kill_all(self):
        self.sort_processes_for_kill()
        for _, process in self.processes:
            # Kill the whole group since the process may spawn a child
            if not process.poll():
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)

    def get_processes_by_name(
        self, name: str
    ) -> Iterable[Tuple[int, subprocess.Popen]]:
        for i, (n, p) in enumerate(self.processes):
            if n == name:
                yield (i, p)

    def kill_process(self, name: str, signal: int = signal.SIGTERM) -> subprocess.Popen:
        for i, p in self.get_processes_by_name(name):
            del self.processes[i]
            # Kill the whole group since the process may spawn a child
            if p.returncode is None and not p.poll():
                os.killpg(os.getpgid(p.pid), signal)
            return p
        else:
            raise Exception("Process not found")

    def sort_processes_for_kill(self):
        pass


class HqEnv(Env):
    def __init__(self, work_dir, mock: ProgramMock, debug=True):
        Env.__init__(self, work_dir)
        self.mock = mock
        self.server = None
        self.workers = {}
        self.id_counter = 0
        self.do_final_check = True
        self.server_dir = ""
        self.debug = debug

    def no_final_check(self):
        self.do_final_check = False

    def make_default_env(self, log=True):
        env = os.environ.copy()
        if log:
            env["RUST_LOG"] = "tako=trace,hyperqueue=trace"
        env["RUST_BACKTRACE"] = "full"
        env["HQ_TEST"] = "1"
        self.mock.update_env(env)
        return env

    @staticmethod
    def server_args(server_dir="hq-server", debug=True):
        args = [
            get_hq_binary(debug=debug),
            "--colors",
            "never",
            "--server-dir",
            server_dir,
        ]
        if debug:
            args.append("--debug")

        args += ["server", "start"]
        return args

    def start_server(
        self, server_dir="hq-server", args=None, env=None
    ) -> subprocess.Popen:
        self.server_dir = os.path.join(self.work_path, server_dir)
        environment = self.make_default_env()
        if env:
            environment.update(env)
        server_args = self.server_args(self.server_dir, debug=self.debug)
        if args:
            server_args += args
        process = self.start_process("server", server_args, env=environment)
        time.sleep(0.2)
        self.check_running_processes()
        return process

    def start_workers(self, count, **kwargs) -> List[subprocess.Popen]:
        workers = []
        for _ in range(count):
            workers.append(self.start_worker(**kwargs))
        return workers

    def start_worker(
        self,
        *,
        cpus="1",
        env=None,
        args=None,
        set_hostname=True,
        wait_for_start=True,
        on_server_lost="stop",
        server_dir=None,
    ) -> subprocess.Popen:
        self.id_counter += 1
        worker_id = self.id_counter
        worker_env = self.make_default_env()
        work_dir = f"workdir{worker_id}"

        if env:
            worker_env.update(env)
        worker_args = [
            get_hq_binary(self.debug),
            "--server-dir",
            server_dir or self.server_dir,
            "worker",
            "start",
            f"--work-dir={work_dir}",
            f"--on-server-lost={on_server_lost}",
            "--no-detect-resources",  # Ignore resources on testing machine
        ]
        hostname = f"worker{worker_id}"
        if set_hostname:
            worker_args += ["--hostname", hostname]
        if cpus is not None:
            worker_args += ["--cpus", str(cpus)]
        if args:
            worker_args += list(args)
        r = self.start_process(hostname, worker_args, env=worker_env)

        if wait_for_start:
            print(wait_for_start)
            assert set_hostname

            def wait_for_worker():
                table = self.command(["worker", "list"], as_table=True)
                print(table)
                return hostname in table.get_column_value("Hostname")

            wait_until(wait_for_worker)
        return r

    def stop_server(self):
        self.command(["server", "stop"])
        for _, p in self.get_processes_by_name("server"):
            p.wait()
            self.check_process_exited(p)

    def kill_server(self):
        self.kill_process("server")

    def kill_worker(self, worker_id: int, signal: int = signal.SIGTERM, wait=True):
        table = self.command(["worker", "info", str(worker_id)], as_table=True)
        pid = table.get_row_value("Process pid")
        process = self.find_process_by_pid(int(pid))
        if process is None:
            raise Exception(f"Worker {worker_id} not found")

        process = self.kill_process(process[0], signal=signal)
        if wait:
            wait_until(lambda: process.poll() is not None)

    def find_process_by_pid(self, pid: int) -> Optional[Tuple[str, subprocess.Popen]]:
        for name, process in self.processes:
            if process.pid == pid:
                return (name, process)
        return None

    def command(
        self,
        args,
        as_table=False,
        as_lines=False,
        as_json=False,
        cwd=None,
        wait=True,
        expect_fail=None,
        stdin=None,
        log=False,
        ignore_stderr=False,
        env=None,
        use_server_dir=True,
    ):
        if isinstance(args, str):
            args = [args]
        else:
            args = list(args)

        if isinstance(stdin, str):
            stdin = stdin.encode()

        final_args = [get_hq_binary(self.debug)]
        if use_server_dir:
            final_args += ["--server-dir", self.server_dir]
        final_args += args
        cwd = cwd or self.work_path
        environment = self.make_default_env(log=log)
        if env is not None:
            environment.update(env)
        stderr = subprocess.DEVNULL if ignore_stderr else subprocess.STDOUT

        if not wait:
            return subprocess.Popen(final_args, stderr=stderr, cwd=cwd, env=environment)

        else:
            process = subprocess.Popen(
                final_args,
                stdout=subprocess.PIPE,
                stderr=stderr,
                cwd=cwd,
                env=environment,
                stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
            )
            stdout = process.communicate(stdin)[0].decode()
            if process.returncode != 0:
                if expect_fail:
                    if expect_fail not in stdout:
                        raise Exception(
                            f"Command should failed with message '{expect_fail}' but got:\n{stdout}"
                        )
                    else:
                        return
                print(f"Process output: {stdout}")
                raise Exception(
                    f"Process failed with exit-code {process.returncode}\n\n{stdout}"
                )
            if expect_fail is not None:
                raise Exception("Command should failed")
            if as_table:
                return parse_tables(stdout)
            if as_lines:
                return stdout.rstrip().split("\n")
            if as_json:
                return json.loads(stdout)
            return stdout

    def final_check(self):
        pass

    def close(self):
        pass

    def sort_processes_for_kill(self):
        # Kill server last to avoid workers ending too soon
        self.processes.sort(key=lambda process: 1 if "server" in process[0] else 0)


@pytest.fixture(autouse=False, scope="function")
def hq_env(tmp_path):
    with run_hq_env(tmp_path) as env:
        yield env


@contextlib.contextmanager
def run_hq_env(tmp_path, debug=True):
    """Fixture that allows to start HQ test environment"""
    print("\nWorking dir", tmp_path)
    os.chdir(tmp_path)

    mock = ProgramMock(tmp_path.joinpath("mock"))
    env = HqEnv(tmp_path, debug=debug, mock=mock)
    yield env
    try:
        env.final_check()
        env.check_running_processes()
    finally:
        env.close()
        env.kill_all()
        # Final sleep to let server port be freed, on some slow computers
        # a new test is starter before the old server is properly cleaned
        time.sleep(0.02)
