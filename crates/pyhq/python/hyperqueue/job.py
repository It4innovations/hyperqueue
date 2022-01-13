from typing import List, Optional

from .task import EnvType, ExternalProgram, ProgramArgs, Task, TaskConfig, build_tasks

JobId = int


class Job:
    def __init__(self, *args, **kwargs):
        self.tasks: List[Task] = []

    def program(
        self,
        args: ProgramArgs,
        env: EnvType = None,
        cwd: Optional[str] = None,
        **kwargs
    ) -> ExternalProgram:
        task = ExternalProgram(args=args, env=env, cwd=cwd)
        self.tasks.append(task)
        return task

    def build(self) -> List[TaskConfig]:
        return build_tasks(self.tasks)
