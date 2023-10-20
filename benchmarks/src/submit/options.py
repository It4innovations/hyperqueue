import dataclasses
import datetime
from pathlib import Path
from typing import Optional

from ..utils.io import from_json, to_json


@dataclasses.dataclass(frozen=True)
class PBSSubmitOptions:
    queue: str
    nodes: int
    walltime: datetime.timedelta
    project: Optional[str] = None
    name: Optional[str] = None
    init_script: Optional[Path] = None


def serialize_submit_options(options: PBSSubmitOptions, path: Path):
    with open(path, "w") as f:
        to_json(options, f)


def deserialize_submit_options(path: Path) -> PBSSubmitOptions:
    with open(path) as f:
        return from_json(PBSSubmitOptions, f)
