
from typing import List
from dataclasses import dataclass
import enum

from tako.client.program import ProgramDefinition


class SubworkerKind(enum.Enum):
    Stateless = "Stateless"
    Stateful = "Stateful"


@dataclass
class SubworkerDefinition:

    id: int
    kind: SubworkerKind
    program: ProgramDefinition

    def as_dict(self):
        return {
            "id": self.id,
            "kind": self.kind.value,
            "args": self.program.as_dict(),
        }
