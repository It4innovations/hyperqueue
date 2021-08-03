from dataclasses import dataclass
from typing import List, Mapping, Optional


@dataclass
class ProgramDefinition:

    args: List[str]
    env: Optional[Mapping[str, str]] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None

    def as_dict(self):
        output = {"args": self.args}
        if self.env:
            output["env"] = self.env
        if self.stdout:
            output["stdout"] = {"File": self.stdout}
        if self.stderr:
            output["stderr"] = {"File": self.stderr}
        return output
