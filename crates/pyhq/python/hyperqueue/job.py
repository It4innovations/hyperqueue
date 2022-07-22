import dataclasses
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

from .common import GenericPath
from .ffi import JobId, TaskId
from .ffi.protocol import JobDescription, ResourceRequest
from .output import default_stderr, default_stdout
from .task.function import PythonFunction
from .task.program import ExternalProgram, ProgramArgs
from .task.task import EnvType, Task


class Job:
    def __init__(
        self,
        default_workdir: Optional[GenericPath] = None,
        max_fails: Optional[int] = 1,
        default_env: Optional[EnvType] = None,
    ):
        self.tasks: List[Task] = []
        self.task_map: Dict[TaskId, Task] = {}
        self.max_fails = max_fails
        self.default_workdir = (
            Path(default_workdir).resolve()
            if default_workdir is not None
            else default_workdir
        )
        self.default_env = default_env or {}

    def task_by_id(self, id: TaskId) -> Optional[Task]:
        return self.task_map.get(id)

    def program(
        self,
        args: ProgramArgs,
        *,
        env: Optional[EnvType] = None,
        cwd: Optional[GenericPath] = None,
        stdout: Optional[GenericPath] = default_stdout(),
        stderr: Optional[GenericPath] = default_stderr(),
        stdin: Optional[Union[str, bytes]] = None,
        deps: Sequence[Task] = (),
        name: Optional[str] = None,
        task_dir: bool = False,
        priority: int = 0,
        resources: Optional[ResourceRequest] = None,
    ) -> ExternalProgram:
        task = ExternalProgram(
            len(self.tasks),
            args=args,
            env=merge_envs(self.default_env, env),
            cwd=cwd or self.default_workdir,
            dependencies=deps,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
            name=name,
            task_dir=task_dir,
            priority=priority,
            resources=resources,
        )
        self._add_task(task)
        return task

    def function(
        self,
        fn,
        *,
        args=(),
        kwargs=None,
        env: Optional[EnvType] = None,
        cwd: Optional[GenericPath] = None,
        stdout: Optional[GenericPath] = default_stdout(),
        stderr: Optional[GenericPath] = default_stderr(),
        deps: Sequence[Task] = (),
        name: Optional[str] = None,
        priority: int = 0,
        resources: Optional[ResourceRequest] = None,
    ) -> PythonFunction:
        task = PythonFunction(
            len(self.tasks),
            fn,
            args=args,
            kwargs=kwargs,
            env=merge_envs(self.default_env, env),
            cwd=cwd or self.default_workdir,
            stdout=stdout,
            stderr=stderr,
            name=name,
            dependencies=deps,
            priority=priority,
            resources=resources,
        )
        self._add_task(task)
        return task

    def _add_task(self, task: Task):
        self.tasks.append(task)
        self.task_map[task.task_id] = task

    def _build(self, client) -> JobDescription:
        task_descriptions = []
        for task in self.tasks:
            task_descriptions.append(task._build(client))
        return JobDescription(task_descriptions, self.max_fails)


@dataclasses.dataclass
class SubmittedJob:
    job: Job
    id: JobId


def merge_envs(default: EnvType, env: Optional[EnvType]) -> EnvType:
    environment = default.copy()
    if env:
        environment.update(env)
    return environment
