import dataclasses
from typing import Dict, List, Optional, Sequence


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


@dataclasses.dataclass
class JobDescription:
    tasks: List[TaskDescription]
