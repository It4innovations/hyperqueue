import dataclasses
from typing import Dict, List, Optional, Sequence


@dataclasses.dataclass()
class ResourceRequest:
    n_nodes: int = 0
    cpus: str = "1"
    generic: Dict[str, int] = dataclasses.field(default_factory=dict)


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
    resource_request: Optional[ResourceRequest]


@dataclasses.dataclass
class JobDescription:
    tasks: List[TaskDescription]
    max_fails: Optional[int]
