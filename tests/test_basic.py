from conftest import HqEnv
import time
import os

def test_empty_stats(hq_env: HqEnv):
    hq_env.start_server()
    print(hq_env.command("stats"))

    # TODO: assert output


def test_submit_out(hq_env: HqEnv, tmp_path):
    hq_env.start_server()
    hq_env.command("submit", "--", "bash", "-c", "echo 'hello'")
    time.sleep(0.2)
    with open(os.path.join(tmp_path, "stdout.1")) as f:
        assert f.read() == "hello"
    with open(os.path.join(tmp_path, "stderr.1")) as f:
        assert f.read() == ""


def test_submit_sleep(hq_env: HqEnv):
    hq_env.start_server()
    print(hq_env.command("submit", "sleep", "1"))
    print(hq_env.command("stats"))
    # TODO: Check task is waiting

    # TODO: Add worker

    # TODO: Check task is task is still waiting
    time.sleep(1.2)

    # TODO: Check task is task is finished

