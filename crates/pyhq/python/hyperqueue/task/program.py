from typing import Dict, List, Optional, Union, Sequence

from ..common import GenericPath
from ..output import Output, gather_outputs
from ..validation import ValidationException, validate_args
from .task import Task

EnvType = Optional[Dict[str, str]]
ProgramArgs = Union[List[str], str]


class ExternalProgram(Task):
    def __init__(
        self,
        args: List[str],
        env: EnvType = None,
        cwd: Optional[GenericPath] = None,
        dependencies: Sequence[Task] = (),
    ):
        super().__init__(dependencies)
        args = to_arg_list(args)
        validate_args(args)
        self.args = args
        self.env = env or {}
        self.cwd = str(cwd) if cwd else None
        self.outputs = get_task_outputs(self)

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
