import pickle
from typing import Callable, Dict

from ..ffi.protocol import TaskDescription
from ..wrapper import CloudWrapper
from .task import Task, TaskId

_CLOUDWRAPPER_CACHE = {}


def purge_cache():
    global _PICKLE_CACHE
    _PICKLE_CACHE = {}


def cloud_wrap(fn, cache=True) -> CloudWrapper:
    if isinstance(fn, CloudWrapper):
        return fn
    return CloudWrapper(fn, cache=cache)


def task_main():
    import sys, pickle
    try:
        fn, a, kw=pickle.loads(sys.stdin.buffer.read())
        fn(*a, **(kw if kw is not None else {}))
    except Exception as e:
        import os, traceback
        t = traceback.format_exc()
        with open(os.environ['HQ_ERROR_FILENAME'], 'w') as f:
            f.write(t)
        sys.exit(1)


class PythonEnv:
    def __init__(self, python_bin="python3", prologue=None, shell="bash"):
        code = (
            "from hyperqueue.task.function import task_main as m; m()"
        )

        if prologue:
            self.args = [shell, "-c", f'{prologue}\n\n{python_bin} -c "{code}"']
        else:
            self.args = [python_bin, "-c", code]


class PythonFunction(Task):
    def __init__(
        self,
        task_id: int,
        fn: Callable,
        *,
        args=(),
        kwargs=None,
        stdout=None,
        stderr=None,
        dependencies=(),
    ):
        super().__init__(task_id, dependencies)

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
        )

    def __repr__(self):
        return f"<PyFunction fn={self.fn.__name__}>"
