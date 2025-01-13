
from conftest import HqEnv

def test_task_data_invalid_call(hq_env: HqEnv):
    hq_env.command(["task-data"])