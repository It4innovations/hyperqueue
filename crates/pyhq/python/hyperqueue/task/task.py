from typing import Optional, Sequence

from ..common import GenericPath
from ..ffi import TaskId
from ..ffi.protocol import ResourceRequest


class Task:
    def __init__(
        self,
        task_id: TaskId,
        dependencies: Sequence["Task"] = (),
        resources: Optional[ResourceRequest] = None,
        cwd: Optional[GenericPath] = None,
        stdout: Optional[GenericPath] = None,
        stderr: Optional[GenericPath] = None,
    ):
        assert dependencies is not None
        self.task_id = task_id
        self.dependencies = dependencies
        self.resources = resources
        self.cwd = str(cwd) if cwd else None
        self.stdout = str(stdout) if stdout else None
        self.stderr = str(stderr) if stdout else None

    def _build(self, client):
        raise NotImplementedError
