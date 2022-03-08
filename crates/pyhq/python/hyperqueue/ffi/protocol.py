import dataclasses
from typing import Dict, List, Optional, Sequence


@dataclasses.dataclass()
class ResourceRequest:
    cpus: str


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
    resource_request: Optional[ResourceRequest]


@dataclasses.dataclass
class JobDescription:
    tasks: List[TaskDescription]
    max_fails: Optional[int]
