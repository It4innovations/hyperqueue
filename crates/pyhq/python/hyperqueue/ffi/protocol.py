import dataclasses
from typing import List, Optional


@dataclasses.dataclass()
class TaskDescription:
    args: List[str]
    cwd: Optional[str]


@dataclasses.dataclass
class JobDescription:
    tasks: List[TaskDescription]
