from typing import Dict, Sequence

TaskId = int


class Task:
    def __init__(self, dependencies: Sequence["Task"] = ()):
        assert dependencies is not None
        self.dependencies = dependencies

    def _build(self, client, id_map: Dict["Task", TaskId]):
        raise NotImplementedError
