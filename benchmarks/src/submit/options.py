import dataclasses
import datetime
from pathlib import Path
from typing import Optional

from mashumaro import DataClassJSONMixin


@dataclasses.dataclass(frozen=True)
class PBSSubmitOptions(DataClassJSONMixin):
    queue: str
    nodes: int
    walltime: datetime.timedelta
    project: Optional[str] = None
    name: Optional[str] = None
    init_script: Optional[Path] = None


def serialize_submit_options(options: PBSSubmitOptions, path: Path):
    with open(path, "w") as f:
        f.write(options.to_json())


def deserialize_submit_options(path: Path) -> PBSSubmitOptions:
    with open(path) as f:
        return PBSSubmitOptions.from_json(f.read())
