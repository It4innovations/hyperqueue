import contextlib
import os
import signal
import subprocess
import time
from typing import Optional, Tuple

import pytest

from .utils import parse_table
from .utils.mock import ProgramMock

PYTEST_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PYTEST_DIR)


def get_hq_binary(debug=True):
    directory = "debug" if debug else "release"
    return os.path.join(ROOT_DIR, "target", directory, "hq")


RUNNING_IN_CI = "CI" in os.environ


class Env:
    def __init__(self, work_path):
        self.processes = []
        self.cleanups = []
        self.work_path = work_path

    def start_process(self, name, args, env=None, catch_io=True, cwd=None):
        cwd = str(cwd or self.work_path)
        logfile = (self.work_path / name).with_suffix(".out")
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
        for (n, p) in self.processes:
            if p is process:
                if process.poll() is None:
                    raise Exception(f"Process with pid {process.pid} is still running")
                if expected_code is not None:
                    assert process.returncode == expected_code

                self.processes = [
                    (n, p) for (n, p) in self.processes if p is not process
                ]
                return
        raise Exception(f"Process with pid {process.pid} not found")

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
        for fn in self.cleanups:
            fn()
        for n, p in self.processes:
            # Kill the whole group since the process may spawn a child
            if not p.poll():
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)

    def kill_process(self, name):
        for i, (n, p) in enumerate(self.processes):
            if n == name:
                del self.processes[i]
                # Kill the whole group since the process may spawn a child
                if p.returncode is None and not p.poll():
                    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                return
        else:
            raise Exception("Process not found")


class HqEnv(Env):
    default_listen_port = 17002

    def __init__(self, work_dir, mock: ProgramMock, debug=True):
        Env.__init__(self, work_dir)
        self.mock = mock
        self.server = None
        self.workers = {}
        self.id_counter = 0
        self.do_final_check = True
        self.server_dir = None
        self.debug = debug

    def no_final_check(self):
        self.do_final_check = False

    def make_default_env(self):
        env = os.environ.copy()
        env["RUST_LOG"] = "tako=trace,hyperqueue=trace"
        env["RUST_BACKTRACE"] = "full"
        self.mock.update_env(env)
        return env

    @staticmethod
    def server_args(server_dir="hq-server", debug=True):
        return [
            get_hq_binary(debug=debug),
            "--colors",
            "never",
            "--server-dir",
            server_dir,
            "server",
            "start",
        ]

    def start_server(self, server_dir="hq-server", args=None) -> subprocess.Popen:
        self.server_dir = os.path.join(self.work_path, server_dir)
        env = self.make_default_env()
        server_args = self.server_args(self.server_dir, debug=self.debug)
        if args:
            server_args += args
        process = self.start_process("server", server_args, env=env)
        time.sleep(0.2)
        self.check_running_processes()
        return process

    def start_workers(self, count, *, sleep=True, **kwargs):
        for _ in range(count):
            self.start_worker(sleep=False, **kwargs)
        if sleep:
            time.sleep(0.2)

    def start_worker(
        self,
        *,
        cpus="1",
        env=None,
        args=None,
        sleep=True,
        set_hostname=True,
    ) -> subprocess.Popen:
        self.id_counter += 1
        worker_id = self.id_counter
        worker_env = self.make_default_env()
        if env:
            worker_env.update(env)
        worker_args = [
            get_hq_binary(self.debug),
            "--server-dir",
            self.server_dir,
            "worker",
            "start",
        ]
        if set_hostname:
            worker_args += ["--hostname", f"worker{worker_id}"]
        if cpus is not None:
            worker_args += ["--cpus", str(cpus)]
        if args:
            worker_args += list(args)
        r = self.start_process(f"worker{worker_id}", worker_args, env=worker_env)
        if sleep:
            time.sleep(0.2)
        return r

    def kill_worker(self, worker_id: int):
        table = self.command(["worker", "info", str(worker_id)], as_table=True)
        pid = table.get_row_value("Process pid")
        process = self.find_process_by_pid(int(pid))
        if process is None:
            raise Exception(f"Worker {worker_id} not found")

        self.kill_process(process[0])

    def find_process_by_pid(self, pid: int) -> Optional[Tuple[str, subprocess.Popen]]:
        for (name, process) in self.processes:
            if process.pid == pid:
                return (name, process)
        return None

    def command(
        self,
        args,
        as_table=False,
        as_lines=False,
        cwd=None,
        wait=True,
        expect_fail=None,
    ):
        if isinstance(args, str):
            args = [args]
        else:
            args = list(args)

        args = [get_hq_binary(self.debug), "--server-dir", self.server_dir] + args
        cwd = cwd or self.work_path
        try:
            if not wait:
                return subprocess.Popen(args, stderr=subprocess.STDOUT, cwd=cwd)

            output = subprocess.check_output(args, stderr=subprocess.STDOUT, cwd=cwd)
            if expect_fail is not None:
                raise Exception("Command should failed")
            output = output.decode()
            if as_table:
                return parse_table(output)
            if as_lines:
                return output.rstrip().split("\n")
            return output
        except subprocess.CalledProcessError as e:
            stdout = e.stdout.decode()
            if expect_fail:
                if expect_fail not in stdout:
                    raise Exception(
                        f"Command should failed with message '{expect_fail}' but got:\n{stdout}"
                    )
                else:
                    return
            print(f"Process output: {stdout}")
            raise Exception(f"Process failed with exit-code {e.returncode}\n\n{stdout}")

    def final_check(self):
        pass

    def close(self):
        pass


@pytest.fixture(autouse=False, scope="function")
def hq_env(tmp_path):
    with run_hq_env(tmp_path) as env:
        yield env


@contextlib.contextmanager
def run_hq_env(tmp_path, debug=True):
    """Fixture that allows to start HQ test environment"""
    print("Working dir", tmp_path)
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
