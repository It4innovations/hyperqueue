from typing import Dict, Sequence

TaskId = int


class Task:
    def __init__(self, dependencies: Sequence["Task"] = ()):
        assert dependencies is not None
        self.dependencies = dependencies

    def build(self, id_map: Dict["Task", TaskId]):
        raise NotImplementedError
