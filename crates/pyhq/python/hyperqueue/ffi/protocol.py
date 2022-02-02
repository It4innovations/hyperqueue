import dataclasses
from typing import Dict, List, Optional


@dataclasses.dataclass()
class TaskDescription:
    id: int
    args: List[str]
    cwd: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]
    stdin: Optional[bytes]
    env: Dict[str, str]
    dependencies: List[int]


@dataclasses.dataclass
class JobDescription:
    tasks: List[TaskDescription]
