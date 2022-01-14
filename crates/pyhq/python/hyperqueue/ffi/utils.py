from ..task import TaskConfig
from .protocol import TaskDescription


def task_config_to_task_desc(config: TaskConfig) -> TaskDescription:
    return TaskDescription(args=config.args, cwd=config.cwd, env=config.env)
