from typing import List, Optional, Sequence, Union

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
        *,
        env: EnvType = None,
        cwd: Optional[GenericPath] = None,
        stdout: Optional[GenericPath] = None,
        stderr: Optional[GenericPath] = None,
        stdin: Optional[Union[str, bytes]] = None,
        depends: Sequence[Task] = (),
    ) -> ExternalProgram:
        task = ExternalProgram(
            args=args,
            env=env,
            cwd=cwd,
            dependencies=depends,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
        )
        self.tasks.append(task)
        return task

    def build(self) -> JobDescription:
        id_map = {task: index for (index, task) in enumerate(self.tasks)}
        task_descriptions = []
        for task in self.tasks:
            task_descriptions.append(task.build(id_map))
        return JobDescription(task_descriptions)
