from typing import List, Optional

import msgpack
from tako.client.program import ProgramDefinition


class Task:

    # __slots__ = ["_id", "type_id", "body", "deps", "keep", "name", "error", "finished"]

    def __init__(
        self,
        type_id: int,
        body: bytes,
        keep: bool,
        deps: Optional[List["Task"]] = None,
        name=None,
    ):
        self._id = None
        self.type_id = type_id
        self.body = body
        self.deps = deps
        self.keep = keep
        self.name = name
        self.finished = False
        self.error = None

    def __repr__(self):
        inner = ""
        if self._id:
            inner += f" id = {self._id}"
        if self.error is not None:
            inner += "error"
        elif self.keep:
            inner += " keep"
        return f"<Task {self.name if self.name else id(self)} {inner}>"


def make_program_task(program_def: ProgramDefinition, keep: bool = False):
    body = msgpack.dumps(program_def.as_dict())
    return Task(0, body, keep)
