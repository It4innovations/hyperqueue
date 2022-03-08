from typing import List, Optional, Sequence, Union

from .common import GenericPath
from .ffi.protocol import JobDescription, ResourceRequest
from .task.function import PythonFunction
from .task.program import EnvType, ExternalProgram, ProgramArgs
from .task.task import Task


class Job:
    def __init__(self, max_fails: Optional[int] = 1):
        self.tasks: List[Task] = []
        self.max_fails = max_fails

    def program(
        self,
        args: ProgramArgs,
        *,
        env: EnvType = None,
        cwd: Optional[GenericPath] = None,
        stdout: Optional[GenericPath] = None,
        stderr: Optional[GenericPath] = None,
        stdin: Optional[Union[str, bytes]] = None,
        dependencies: Sequence[Task] = (),
        task_dir: bool = False,
        resources: Optional[ResourceRequest] = None,
    ) -> ExternalProgram:
        task = ExternalProgram(
            len(self.tasks),
            args=args,
            env=env,
            cwd=cwd,
            dependencies=dependencies,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
            task_dir=task_dir,
            resources=resources,
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
        dependencies: Sequence[Task] = (),
        resources: Optional[ResourceRequest] = None,
    ):
        task = PythonFunction(
            len(self.tasks),
            fn,
            args=args,
            kwargs=kwargs,
            stdout=stdout,
            stderr=stderr,
            dependencies=dependencies,
            resources=resources,
        )
        self.tasks.append(task)
        return task

    def _build(self, client) -> JobDescription:
        task_descriptions = []
        for task in self.tasks:
            task_descriptions.append(task._build(client))
        return JobDescription(task_descriptions, self.max_fails)
