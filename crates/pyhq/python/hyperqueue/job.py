from typing import List, Optional

from .common import GenericPath
from .task.config import TaskConfig, build_tasks
from .task.program import EnvType, ExternalProgram, ProgramArgs
from .task.task import Task

JobId = int


class Job:
    def __init__(self, *args, **kwargs):
        self.tasks: List[Task] = []

    def program(
        self,
        args: ProgramArgs,
        env: EnvType = None,
        cwd: Optional[GenericPath] = None,
        depends: List[Task] = None,
        **kwargs
    ) -> ExternalProgram:
        task = ExternalProgram(args=args, env=env, cwd=cwd, dependencies=depends)
        self.tasks.append(task)
        return task

    def build(self) -> List[TaskConfig]:
        return build_tasks(self.tasks)
