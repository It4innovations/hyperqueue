import subprocess
import pytest
import os
import signal
import time

from .testutils import parse_table

PYTEST_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PYTEST_DIR)
HQ_BINARY = os.path.join(ROOT_DIR, "target", "debug", "hq")
HQ_WORKER_BIN = os.path.join(ROOT_DIR, "target", "debug", "hq-worker")


class Env:

    def __init__(self, work_path):
        self.processes = []
        self.cleanups = []
        self.work_path = work_path

    def start_process(self, name, args, env=None, catch_io=True, cwd=None, detach=False):
        cwd = str(cwd or self.work_path)
        logfile = (self.work_path / name).with_suffix(".out")
        if catch_io:
            with open(logfile, "w") as out:
                p = subprocess.Popen(args,
                                     preexec_fn=os.setsid,
                                     stdout=out,
                                     stderr=subprocess.STDOUT,
                                     cwd=cwd,
                                     env=env)
        else:
            p = subprocess.Popen(args,
                                 cwd=cwd,
                                 env=env)
        if not detach:
            self.processes.append((name, p))
        return p

    def check_running_processes(self):
        """Checks that everything is still running"""
        for name, process in self.processes:
            if process.poll() is not None:
                raise Exception(
                    "Process {0} crashed (log in {1}/{0}.out)".format(name, self.work_path))

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

    def __init__(self, work_dir):
        Env.__init__(self, work_dir)
        self.server = None
        self.workers = {}
        self.id_counter = 0
        self.do_final_check = True
        self.server_dir = None

    def no_final_check(self):
        self.do_final_check = False

    def make_default_env(self):
        env = os.environ.copy()
        env["RUST_LOG"] = "trace"
        env["RUST_BACKTRACE"] = "full"
        return env

    def start_server(self, server_dir="./hq-server"):
        self.server_dir = os.path.join(self.work_path, server_dir)
        env = self.make_default_env()
        args = [HQ_BINARY, "--server-dir", self.server_dir, "server", "start"]
        self.start_process("server", args, env=env)
        time.sleep(0.2)
        self.check_running_processes()

    def start_worker(self, *, n_cpus=None, detach=False):
        self.id_counter += 1
        worker_id = self.id_counter
        env = self.make_default_env()
        args = [HQ_BINARY, "--server-dir", self.server_dir, "worker", "start"]
        if n_cpus is not None:
            args += ["--cpus", str(n_cpus)]
        return self.start_process(f"worker{worker_id}", args, env=env, detach=detach)

    def kill_worker(self, worker_id):
        self.kill_process(f"worker{worker_id}")

    def command(self, args, as_table=False):
        if isinstance(args, str):
            args = [args]
        else:
            args = list(args)

        args = [HQ_BINARY, "--server-dir", self.server_dir] + args
        try:
            output = subprocess.check_output(args, stderr=subprocess.STDOUT, cwd=self.work_path)
            output = output.decode()
            if as_table:
                return parse_table(output)
            return output
        except subprocess.CalledProcessError as e:
            print("Process output: ", e.stdout)
            raise

    def final_check(self):
        pass

    def close(self):
        pass


@pytest.fixture(autouse=False, scope="function")
def hq_env(tmp_path):
    """Fixture that allows to start HQ test environment"""
    print("Working dir", tmp_path)
    os.chdir(tmp_path)
    env = HqEnv(tmp_path)
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
