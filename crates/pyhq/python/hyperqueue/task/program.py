from typing import Dict, List, Optional, Sequence, Union

from ..common import GenericPath
from ..ffi import TaskId
from ..ffi.protocol import ResourceRequest, TaskDescription
from ..output import Output, gather_outputs
from ..validation import ValidationException, validate_args
from .task import Task

EnvType = Optional[Dict[str, str]]
ProgramArgs = Union[List[str], str]


class ExternalProgram(Task):
    def __init__(
        self,
        task_id: TaskId,
        *,
        args: List[str],
        env: EnvType = None,
        cwd: Optional[GenericPath] = None,
        stdout: Optional[GenericPath] = None,
        stderr: Optional[GenericPath] = None,
        stdin: Optional[Union[str, bytes]] = None,
        dependencies: Sequence[Task] = (),
        task_dir: bool = False,
        resources: Optional[ResourceRequest],
    ):
        super().__init__(task_id, dependencies, resources)
        args = to_arg_list(args)
        validate_args(args)
        self.args = args
        self.env = env or {}
        self.cwd = str(cwd) if cwd else None
        self.stdout = str(stdout) if stdout else None
        self.stderr = str(stderr) if stdout else None
        self.task_dir = task_dir

        if stdin is None or isinstance(stdin, bytes):
            self.stdin = stdin
        elif isinstance(stdin, str):
            self.stdin = stdin.encode()
        else:
            raise Exception("stdin has to be str, bytes, or None")

        self.outputs = get_task_outputs(self)

    def _build(self, client):
        depends_on = [dependency.task_id for dependency in self.dependencies]
        return TaskDescription(
            id=self.task_id,
            args=self.args,
            env=self.env,
            stdout=self.stdout,
            stderr=self.stderr,
            stdin=self.stdin,
            cwd=self.cwd,
            dependencies=depends_on,
            task_dir=self.task_dir,
            resource_request=self.resources,
        )

    def __getitem__(self, key: str):
        if key not in self.outputs:
            raise Exception(f"Output `{key}` not found in {self}")
        return self.outputs[key]

    def __repr__(self):
        return f"Task(args={self.args}, env={self.env}, cwd={self.cwd}, outputs={self.outputs})"


def to_arg_list(args: ProgramArgs) -> List[str]:
    if isinstance(args, str):
        return [args]
    return args


def get_task_outputs(task: ExternalProgram) -> Dict[str, Output]:
    # TODO: outputs in cwd
    # TODO: multiple outputs with the same name, but different parameters
    output_map = {}

    outputs = gather_outputs(task.args) + gather_outputs(task.env)
    for output in outputs:
        if output.name in output_map:
            raise ValidationException(
                f"Output `{output.name}` has been defined multiple times"
            )
        output_map[output.name] = output
    return output_map
