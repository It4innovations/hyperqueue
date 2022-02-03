from typing import Callable, Dict

import cloudpickle
import pickle

from ..ffi.protocol import TaskDescription
from .task import Task, TaskId

_PICKLE_CACHE = {}


def serialize(obj):
    obj_id = id(obj)
    result = _PICKLE_CACHE.get(obj_id)
    if result is None:
        result = cloudpickle.dumps(obj)
        _PICKLE_CACHE[obj_id] = result
    return result


def purge_cache():
    global _PICKLE_CACHE
    _PICKLE_CACHE = {}


class PythonFunction(Task):
    def __init__(self, fn: Callable, *, args=(), kwargs=None, stdout=None, stderr=None, dependencies=()):
        super().__init__(dependencies)
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.stdout = stdout
        self.stderr = stderr

    def _build(self, client, id_map: Dict[Task, TaskId]) -> TaskDescription:
        depends_on = [id_map[dependency] for dependency in self.dependencies]
        serialized_fn = serialize(self.fn)
        return TaskDescription(
            id=id_map[self],
            args=[
                client.python_bin,
                "-c",
                "import sys;import cloudpickle;import pickle;"
                "fn,a,kw=pickle.loads(sys.stdin.buffer.read());"
                "fn=cloudpickle.loads(fn);"
                "fn(*a, **(kw if kw is not None else {}))",
            ],
            stdout=self.stdout,
            stderr=self.stderr,
            env={},
            stdin=pickle.dumps((serialized_fn, self.args, self.kwargs)),
            cwd=None,
            dependencies=depends_on,
        )
