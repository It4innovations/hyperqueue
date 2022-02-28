from typing import Dict, Sequence

TaskId = int


class Task:
    def __init__(self, task_id: int, dependencies: Sequence["Task"] = ()):
        assert dependencies is not None
        self.task_id = task_id
        self.dependencies = dependencies

    def _build(self, client):
        raise NotImplementedError
