from typing import Sequence

TaskId = int


class Task:

    def __init__(self, dependencies: Sequence["Task"] = ()):
        assert dependencies is not None
        self.dependencies = dependencies
