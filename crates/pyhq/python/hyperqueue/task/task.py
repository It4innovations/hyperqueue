from pathlib import Path
from typing import Dict, Optional, Sequence, Union

from ..common import GenericPath
from ..ffi import TaskId
from ..ffi.protocol import ResourceRequest
from ..output import Stdio, StdioDef
from ..validation import ValidationException

EnvType = Dict[str, str]


def _make_ffi_requests(
    resources: Optional[Union[ResourceRequest, Sequence[ResourceRequest]]],
):
    if resources is None:
        return ()
    elif isinstance(resources, ResourceRequest):
        return (resources,)
    else:
        return resources


class Task:
    def __init__(
        self,
        task_id: TaskId,
        dependencies: Sequence["Task"] = (),
        priority: int = 0,
        resources: Optional[ResourceRequest] = None,
        env: Optional[EnvType] = None,
        cwd: Optional[GenericPath] = None,
        stdout: Optional[Stdio] = None,
        stderr: Optional[Stdio] = None,
        name: Optional[str] = None,
    ):
        assert dependencies is not None
        self.task_id = task_id
        self.dependencies = dependencies
        self.priority = priority
        self.resources = resources
        self.env = env or {}
        self.cwd = str(cwd) if cwd else None
        self.stdout = build_stdio(stdout, "stdout")
        self.stderr = build_stdio(stderr, "stderr")
        self.name = name

    @property
    def label(self) -> str:
        """
        Returns the label of the task.
        If the task has an assigned name, the label is equal to the name.
        Otherwise, the label is the ID of the task converted to a string.
        """
        return self.name if self.name is not None else str(self.task_id)

    def _build(self, client):
        raise NotImplementedError


def build_stdio(stdio: Stdio, stream: str) -> StdioDef:
    if isinstance(stdio, (str, Path)):
        return StdioDef.from_path(stdio)
    elif isinstance(stdio, StdioDef):
        return stdio
    else:
        raise ValidationException(
            f"Invalid value provided for `{stream}`: {type(stdio)}. Expected str, Path or `StdioDef`."
        )
