import logging
import os
import pickle
from typing import Callable, Optional, Sequence, Union

from ...common import GenericPath
from ...ffi import TaskId
from ...ffi.protocol import ResourceRequest, TaskDescription
from ..task import EnvType, Task, _make_ffi_requests
from .wrapper import CloudWrapper

_CLOUDWRAPPER_CACHE = {}


def purge_cache():
    global _CLOUDWRAPPER_CACHE
    _CLOUDWRAPPER_CACHE = {}


def cloud_wrap(fn, cache=True) -> CloudWrapper:
    if isinstance(fn, CloudWrapper):
        return fn
    return CloudWrapper(fn, cache=cache)


LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


def get_logging_level(default: int) -> int:
    level = os.environ.get("HQ_PYLOG")
    return LOG_LEVELS.get(level, default)


def task_main():
    import pickle
    import sys

    try:
        logging.basicConfig(
            level=get_logging_level(logging.INFO),
            format="%(asctime)s %(name)s:%(levelname)-4s %(message)s",
            datefmt="%d-%m-%Y %H:%M:%S",
        )

        fn, a, kw = pickle.loads(sys.stdin.buffer.read())
        fn(*a, **(kw if kw is not None else {}))
    except BaseException:
        import os
        import traceback

        t = traceback.format_exc()
        with open(os.environ["HQ_ERROR_FILENAME"], "w") as f:
            f.write(t)
        print(t, file=sys.stderr)
        sys.stdout.flush()
        sys.stderr.flush()
        sys.exit(1)


class PythonEnv:
    def __init__(self, python_bin="python3", prologue=None, shell="bash"):
        code = "from hyperqueue.task.function import task_main as m; m()"

        if prologue:
            self.args = [shell, "-c", f'{prologue}\n\n{python_bin} -c "{code}"']
        else:
            self.args = [python_bin, "-c", code]


class PythonFunction(Task):
    """
    Task that represents the execution of a Python function.
    """

    def __init__(
        self,
        task_id: TaskId,
        fn: Callable,
        *,
        args=(),
        kwargs=None,
        env: Optional[EnvType] = None,
        cwd: Optional[GenericPath] = None,
        stdout: Optional[GenericPath] = None,
        stderr: Optional[GenericPath] = None,
        name: Optional[str] = None,
        dependencies=(),
        priority: int = 0,
        resources: Optional[Union[ResourceRequest, Sequence[ResourceRequest]]] = None,
    ):
        name = generate_task_name(task_id, name, fn)
        super().__init__(
            task_id,
            dependencies,
            priority,
            resources,
            env=env,
            cwd=cwd,
            stdout=stdout,
            stderr=stderr,
            name=name,
        )

        fn_id = id(fn)
        wrapper = _CLOUDWRAPPER_CACHE.get(fn_id)
        if wrapper is None:
            wrapper = cloud_wrap(fn)
            _CLOUDWRAPPER_CACHE[fn_id] = wrapper

        self.fn = wrapper
        self.args = args
        self.kwargs = kwargs

    def _build(self, client) -> TaskDescription:
        depends_on = [dependency.task_id for dependency in self.dependencies]
        return TaskDescription(
            id=self.task_id,
            args=client.python_env.args,
            stdout=self.stdout,
            stderr=self.stderr,
            stdin=pickle.dumps((self.fn, self.args, self.kwargs)),
            env=self.env,
            cwd=self.cwd,
            dependencies=depends_on,
            task_dir=True,
            priority=self.priority,
            resource_request=_make_ffi_requests(self.resources),
        )

    def __repr__(self):
        return f"<PyFunction fn={self.fn.__name__}>"


def generate_task_name(id: TaskId, name: Optional[str], fn: Callable) -> Optional[str]:
    if name is not None:
        return name

    if hasattr(fn, "__name__"):
        return f"{fn.__name__}/{id}"
    return None
