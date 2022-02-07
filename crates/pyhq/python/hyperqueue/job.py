from typing import List, Optional, Sequence, Union

from .common import GenericPath
from .ffi.protocol import JobDescription
from .task.function import PythonFunction
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

    def function(
        self,
        fn,
        *,
        args=(),
        kwargs=None,
        stdout: Optional[GenericPath] = None,
        stderr: Optional[GenericPath] = None,
    ):
        task = PythonFunction(
            fn, args=args, kwargs=kwargs, stdout=stdout, stderr=stderr
        )
        self.tasks.append(task)
        return task

    def _build(self, client) -> JobDescription:
        id_map = {task: index for (index, task) in enumerate(self.tasks)}
        task_descriptions = []
        for task in self.tasks:
            task_descriptions.append(task._build(client, id_map))
        return JobDescription(task_descriptions)
