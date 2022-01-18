from ..task.config import TaskConfig
from .protocol import TaskDescription


def task_config_to_task_desc(config: TaskConfig) -> TaskDescription:
    return TaskDescription(
        id=config.id,
        args=config.args,
        cwd=config.cwd,
        env=config.env,
        dependencies=config.dependencies,
    )
