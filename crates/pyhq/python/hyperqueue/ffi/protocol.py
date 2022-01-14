import dataclasses
from typing import Dict, List, Optional


@dataclasses.dataclass()
class TaskDescription:
    args: List[str]
    cwd: Optional[str]
    env: Dict[str, str]


@dataclasses.dataclass
class JobDescription:
    tasks: List[TaskDescription]
