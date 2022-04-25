import pickle
from typing import Callable, Optional

from ..ffi import TaskId
from ..ffi.protocol import ResourceRequest, TaskDescription
from ..wrapper import CloudWrapper
from .task import Task

_CLOUDWRAPPER_CACHE = {}


def purge_cache():
    global _PICKLE_CACHE
    _PICKLE_CACHE = {}


def cloud_wrap(fn, cache=True) -> CloudWrapper:
    if isinstance(fn, CloudWrapper):
        return fn
    return CloudWrapper(fn, cache=cache)


def task_main():
    import pickle
    import sys

    try:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)s:%(levelname)-4s %(message)s",
            datefmt="%d-%m-%Y %H:%M:%S",
        )

        fn, a, kw = pickle.loads(sys.stdin.buffer.read())
        fn(*a, **(kw if kw is not None else {}))
    except Exception:
        import os
        import traceback

        t = traceback.format_exc()
        with open(os.environ["HQ_ERROR_FILENAME"], "w") as f:
            f.write(t)
        sys.exit(1)


class PythonEnv:
    def __init__(self, python_bin="python3", prologue=None, shell="bash"):
        code = "from hyperqueue.task.function import task_main as m; m()"

        if prologue:
            self.args = [shell, "-c", f'{prologue}\n\n{python_bin} -c "{code}"']
        else:
            self.args = [python_bin, "-c", code]


class PythonFunction(Task):
    def __init__(
        self,
        task_id: TaskId,
        fn: Callable,
        *,
        args=(),
        kwargs=None,
        stdout=None,
        stderr=None,
        dependencies=(),
        resources: Optional[ResourceRequest] = None,
    ):
        super().__init__(task_id, dependencies, resources)

        fn_id = id(fn)
        wrapper = _CLOUDWRAPPER_CACHE.get(fn_id)
        if wrapper is None:
            wrapper = cloud_wrap(fn)
            _CLOUDWRAPPER_CACHE[fn_id] = wrapper

        self.fn = wrapper
        self.args = args
        self.kwargs = kwargs
        self.stdout = stdout
        self.stderr = stderr

    def _build(self, client) -> TaskDescription:
        depends_on = [dependency.task_id for dependency in self.dependencies]
        return TaskDescription(
            id=self.task_id,
            args=client.python_env.args,
            stdout=self.stdout,
            stderr=self.stderr,
            env={},
            stdin=pickle.dumps((self.fn, self.args, self.kwargs)),
            cwd=None,
            dependencies=depends_on,
            task_dir=True,
            resource_request=self.resources,
        )

    def __repr__(self):
        return f"<PyFunction fn={self.fn.__name__}>"
