import os
import os.path
import signal
import subprocess
import sys
import time
from socket import gethostname

import psutil
import pytest

PYTEST_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PYTEST_DIR)
TAKO_SERVER_BIN = os.path.join(ROOT_DIR, "target", "debug", "tako-server")
TAKO_WORKER_BIN = os.path.join(ROOT_DIR, "target", "debug", "tako-worker")
TAKO_PYTHON = os.path.join(ROOT_DIR, "python")

sys.path.insert(0, TAKO_PYTHON)


def check_free_port(port):
    assert isinstance(port, int)
    for conn in psutil.net_connections("tcp"):
        if conn.laddr.port == port and conn.status == "LISTEN":
            return False
    return True


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

    def expect_exit(self, process, non_zero_exit_code=False):
        for i, (n, p) in enumerate(self.processes):
            if p == process:
                if process.returncode is None:
                    p.poll()
                if non_zero_exit_code:
                    assert process.returncode is not None and process.returncode != 0
                else:
                    assert process.returncode is not None
                del self.processes[i]
                return
        else:
            raise Exception("Process not found")

    def kill_process(self, process):
        for i, (n, p) in enumerate(self.processes):
            if p == process:
                del self.processes[i]
                # Kill the whole group since the process may spawn a child
                if p.returncode is None and not p.poll():
                    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                return
        else:
            raise Exception("Process not found")

    def kill_all(self):
        for fn in self.cleanups:
            fn()
        for n, p in self.processes:
            # Kill the whole group since the process may spawn a child
            if p.returncode is None and not p.poll():
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)


class TakoEnv(Env):
    default_listen_port = 17002

    def __init__(self, work_dir):
        Env.__init__(self, work_dir)
        self.server = None
        self.workers = {}
        self.id_counter = 0
        self.do_final_check = True
        self.server_socket_path = os.path.join(work_dir, "server.sock")
        self.work_dir = work_dir
        self._session = None

    def pause_worker(self, id):
        worker = self.workers.pop(id)
        assert worker is not None
        os.kill(worker.pid, signal.SIGSTOP)

    def kill_worker(self, id):
        worker = self.workers.pop(id)
        assert worker is not None
        self.kill_process(worker)

    def expect_worker_fail(self, id):
        worker = self.workers.pop(id)
        assert worker is not None
        self.expect_exit(worker, non_zero_exit_code=True)

    def expect_worker_exit(self, id):
        worker = self.workers.pop(id)
        assert worker is not None
        self.expect_exit(worker)

    def no_final_check(self):
        self.do_final_check = False

    def make_env(self):
        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "full"
        env["RUST_LOG"] = "debug"
        return env

    def start_worker(
        self,
        ncpus,
        port=None,
        heartbeat=None,
        secret_file=None,
        nsockets=None,
        hw_interval=None,
        time_limit=None,
        extra_args=None,
    ):
        port = port or self.default_listen_port
        worker_id = self.id_counter
        self.id_counter += 1
        name = "worker{}".format(worker_id)
        program = TAKO_WORKER_BIN
        env = self.make_env()
        python_path = [PYTEST_DIR]
        if "PYTHONPATH" in env:
            python_path.append(env["PYTHONPATH"])
        python_path.append(TAKO_PYTHON)
        env["PYTHONPATH"] = ":".join(python_path)

        work_dir = self.work_path / name
        work_dir.mkdir()
        args = [
            program,
            f"{gethostname()}:{port}",
            "--cpus",
            str(ncpus),
            "--work-dir",
            work_dir,
        ]

        if extra_args:
            args += extra_args

        if hw_interval is not None:
            args.extend(["--hw-state-poll-interval", str(hw_interval)])

        if nsockets is not None:
            args.append("--sockets")
            args.append(str(nsockets))

        if heartbeat:
            args.append("--heartbeat")
            args.append(str(heartbeat))

        if time_limit:
            args.append("--time-limit")
            args.append(str(time_limit))

        if secret_file:
            args.append("--secret-file")
            args.append(secret_file)

        self.workers[worker_id] = self.start_process(name, args, env, cwd=work_dir)
        # else:
        #    program = "dask-worker"
        #    args = [program, "localhost:{}".format(port), "--nthreads", str(ncpus)]
        #    self.workers[name] = self.start_process(name, args, env)

    def create_secret_file(self, filename, secret):
        with open(os.path.join(self.work_path, filename), "w") as f:
            f.write(secret)

    def start_server(
        self, port=None, panic_on_worker_lost=None, secret_file=None, idle_timeout=None
    ):
        if self.server:
            raise Exception("Server is already running")

        port = port or self.default_listen_port

        if not check_free_port(port):
            raise Exception(
                "Trying to spawn server on port {}, but it is not free".format(port)
            )

        env = self.make_env()

        args = [TAKO_SERVER_BIN, "--port", str(port), self.server_socket_path]

        if panic_on_worker_lost:
            args.append("--panic-on-worker-lost")
        if secret_file:
            args.append("--secret-file")
            args.append(secret_file)
        if idle_timeout:
            args.append("--idle-timeout")
            args.append(str(idle_timeout))

        self.server = self.start_process("server", args, env=env)
        assert self.server is not None
        return self.server

    def start(
        self,
        workers=(),
        port=None,
        worker_start_delay=None,
        panic_on_worker_lost=True,
        heartbeat=None,
        secret_file=None,
        idle_timeout=None,
        hw_interval=None,
        time_limit=None,
    ):
        print("Starting tako env in ", self.work_path)

        """
        Start infrastructure: server & n governors
        """
        port = port or self.default_listen_port
        self.start_server(
            port=port,
            panic_on_worker_lost=panic_on_worker_lost,
            secret_file=secret_file,
            idle_timeout=idle_timeout,
        )

        it = 0
        while check_free_port(port):
            time.sleep(0.05)
            self.check_running_processes()
            it += 1
            if it > 100:
                raise Exception("Server not started after 5")

        for cpus in workers:
            self.start_worker(
                cpus,
                port=port,
                heartbeat=heartbeat,
                hw_interval=hw_interval,
                time_limit=time_limit,
            )
            if worker_start_delay:
                time.sleep(worker_start_delay)

        time.sleep(0.2)  # TODO: Replace with real check that worker is

        self.check_running_processes()
        return self.session()

    def check_running_processes(self):
        """Checks that everything is still running"""
        for worker_name, worker in self.workers.items():
            if worker.poll() is not None:
                raise Exception(
                    "worker{0} crashed (log in {1}/worker{0}.out)".format(
                        worker_name, self.work_path
                    )
                )

        if self.server and self.server.poll() is not None:
            server = self.server
            self.server = None
            if server.returncode != 0:
                raise Exception(
                    "Server crashed (log in {}/server.out)".format(self.work_path)
                )

    def session(self):
        assert self._session is None
        from tako.client import connect

        session = connect(self.server_socket_path)
        self._session = session
        return session

    def final_check(self):
        if self._session:
            overview = self._session.overview()
            for w in overview["worker_overviews"]:
                assert len(w["running_tasks"]) == 0
                assert len(w["placed_data"]) == 0

    def close(self):
        pass

    def expect_server_fail(self):
        self.expect_exit(self.server, non_zero_exit_code=True)
        self.server = None


@pytest.fixture(autouse=False, scope="function")
def tako_env(tmp_path):
    """Fixture that allows to start Rain test environment"""
    os.chdir(tmp_path)
    env = TakoEnv(tmp_path)
    try:
        yield env
        time.sleep(0.2)
        env.final_check()
        env.check_running_processes()
    finally:
        env.close()
        env.kill_all()
        # Final sleep to let server port be freed, on some slow computers
        # a new test is starter before the old server is properly cleaned
        time.sleep(0.02)
