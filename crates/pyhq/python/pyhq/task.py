import dataclasses
from typing import Dict, List, Optional, Union

from .output import Output, gather_outputs
from .validation import ValidationException, validate_args


EnvType = Optional[Dict[str, str]]
ProgramArgs = Union[List[str], str]


class Task:
    pass


class ExternalProgram(Task):
    def __init__(self, args: List[str], env: EnvType = None, cwd: Optional[str] = None):
        args = to_arg_list(args)
        validate_args(args)
        self.args = args
        self.env = env or {}
        self.cwd = cwd
        self.outputs = get_task_outputs(self)

    def __getitem__(self, key: str):
        if key not in self.outputs:
            raise Exception(f"Output `{key}` not found in {self}")
        return self.outputs[key]

    def __repr__(self):
        return f"Task(args={self.args}, env={self.env}, cwd={self.cwd}, outputs={self.outputs})"


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


@dataclasses.dataclass
class TaskConfig:
    id: int
    args: List[str]
    env: Dict[str, str]
    cwd: Optional[List[str]]


# ["cp", task["output"], ""] -> ["cp", "%{task:1000-HQ_TASK_ID}.txt", ""]

# {task:[100:1000]/TASK_ID}

# JOB_ID - OK
# TASK_ID - OK


def build_tasks(tasks: List[ExternalProgram]) -> List[TaskConfig]:
    id_map = {task: index for (index, task) in enumerate(tasks)}
    configs = []
    for task in tasks:
        configs.append(
            TaskConfig(id=id_map[task], args=task.args, env=task.env, cwd=task.cwd)
        )
    return configs


def to_arg_list(args: ProgramArgs) -> List[str]:
    if isinstance(args, str):
        return [args]
    return args
