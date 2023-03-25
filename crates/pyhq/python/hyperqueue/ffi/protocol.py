import dataclasses
from typing import Dict, List, Optional, Sequence, Union


class ResourceRequest:
    n_nodes: int = 0
    resources: Dict[str, Union[int, str]] = dataclasses.field(default_factory=dict)

    def __init__(
        self,
        *,
        n_nodes=0,
        cpus: Union[int, str] = 1,
        resources: Optional[Dict[str, Union[int, str]]] = None,
    ):
        self.n_nodes = n_nodes
        if resources is None:
            resources = {}
        resources["cpus"] = cpus
        self.resources = resources

    def __repr__(self):
        return f"<ResourceRequest n_nodes={self.n_nodes} resources={self.resources}>"


@dataclasses.dataclass()
class TaskDescription:
    id: int
    args: List[str]
    cwd: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]
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
