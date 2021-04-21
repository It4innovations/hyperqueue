import subprocess
import pytest
import os
import signal
import time

PYTEST_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(PYTEST_DIR)
HQ_BINARY = os.path.join(ROOT_DIR, "target", "debug", "hq")
HQ_WORKER_BIN = os.path.join(ROOT_DIR, "target", "debug", "hq-worker")

TAKO_BIN_PATH = os.path.join(os.path.dirname(ROOT_DIR), "tako", "target", "debug")
#TAKO_SERVER_BIN = os.path.join(TAKO_BIN_PATH, "tako-server")


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


class HqEnv(Env):
    default_listen_port = 17002

    def __init__(self, work_dir):
        Env.__init__(self, work_dir)
        self.server = None
        self.workers = {}
        self.id_counter = 0
        self.do_final_check = True

    def no_final_check(self):
        self.do_final_check = False

    def make_default_env(self):
        env = os.environ.copy()
        env["RUST_LOG"] = "trace"
        env["RUST_BACKTRACE"] = "full"
        return env

    def start_server(self):
        env = self.make_default_env()
        env["PATH"] = env.get("PATH", "") + ":" + TAKO_BIN_PATH
        args = [HQ_BINARY, "hq-server.socket"]
        self.start_process("server", args, env=env)
        self.check_running_processes()
        time.sleep(0.2)

    def command(self, *args):
        args = [HQ_BINARY] + list(args)
        try:
            output = subprocess.check_output(args, stderr=subprocess.STDOUT, cwd=self.work_path)
            return output.decode()
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