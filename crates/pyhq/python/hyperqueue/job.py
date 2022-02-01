from typing import List, Optional, Sequence

from .common import GenericPath
from .ffi.protocol import JobDescription, TaskDescription
from .task.program import EnvType, ExternalProgram, ProgramArgs
from .task.task import Task

JobId = int


class Job:
    def __init__(self):
        self.tasks: List[Task] = []

    def program(
        self,
        args: ProgramArgs,
        env: EnvType = None,
        cwd: Optional[GenericPath] = None,
        depends: Sequence[Task] = (),
    ) -> ExternalProgram:
        task = ExternalProgram(args=args, env=env, cwd=cwd, dependencies=depends)
        self.tasks.append(task)
        return task

    def build(self) -> JobDescription:
        id_map = {task: index for (index, task) in enumerate(self.tasks)}
        task_descriptions = []
        for task in self.tasks:
            depends_on = [id_map[dependency] for dependency in task.dependencies]
            task_descriptions.append(
                TaskDescription(
                    id=id_map[task],
                    args=task.args,
                    env=task.env,
                    cwd=task.cwd,
                    dependencies=depends_on,
                )
            )
        return JobDescription(task_descriptions)
