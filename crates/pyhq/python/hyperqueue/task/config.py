import dataclasses
from typing import Dict, List, Optional

from .program import ExternalProgram
from .task import TaskId


@dataclasses.dataclass
class TaskConfig:
    id: TaskId
    args: List[str]
    env: Dict[str, str]
    cwd: Optional[List[str]]
    dependencies: List[TaskId]


def build_tasks(tasks: List[ExternalProgram]) -> List[TaskConfig]:
    id_map = {task: index for (index, task) in enumerate(tasks)}
    configs = []
    for task in tasks:
        depends_on = [id_map[dependency] for dependency in task.dependencies]
        configs.append(
            TaskConfig(
                id=id_map[task],
                args=task.args,
                env=task.env,
                cwd=task.cwd,
                dependencies=depends_on,
            )
        )
    return configs
