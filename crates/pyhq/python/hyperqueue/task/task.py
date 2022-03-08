from typing import Optional, Sequence

from hyperqueue.ffi.protocol import ResourceRequest

TaskId = int


class Task:
    def __init__(
        self,
        task_id: int,
        dependencies: Sequence["Task"] = (),
        resources: Optional[ResourceRequest] = None,
    ):
        assert dependencies is not None
        self.task_id = task_id
        self.dependencies = dependencies
        self.resources = resources

    def _build(self, client):
        raise NotImplementedError
