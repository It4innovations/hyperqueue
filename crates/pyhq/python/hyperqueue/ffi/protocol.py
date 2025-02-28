import dataclasses
import datetime
from typing import Dict, List, Optional, Sequence, Union

from ..output import StdioDef


class ResourceRequest:
    n_nodes: int = 0
    resources: Dict[str, Union[int, float, str]] = dataclasses.field(default_factory=dict)
    min_time: Optional[datetime.timedelta] = None

    def __init__(
        self,
        *,
        n_nodes=0,
        cpus: Union[int, float, str] = 1,
        resources: Optional[Dict[str, Union[int, float, str]]] = None,
        min_time: Optional[datetime.timedelta] = None,
    ):
        self.n_nodes = n_nodes
        if resources is None:
            resources = {}
        resources["cpus"] = cpus
        self.resources = resources
        self.min_time = min_time

    def __repr__(self):
        return f"<ResourceRequest n_nodes={self.n_nodes} resources={self.resources} min_time={self.min_time}>"


@dataclasses.dataclass()
class TaskDescription:
    id: int
    args: List[str]
    cwd: Optional[str]
    stdout: Optional[StdioDef]
    stderr: Optional[StdioDef]
    stdin: Optional[bytes]
    env: Optional[Dict[str, str]]
    dependencies: Sequence[int]
    task_dir: bool
    priority: int
    resource_request: Sequence[ResourceRequest]


@dataclasses.dataclass
class JobDescription:
    tasks: List[TaskDescription]
    max_fails: Optional[int]
