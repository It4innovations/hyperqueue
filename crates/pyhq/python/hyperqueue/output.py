import dataclasses
import uuid
from typing import List, Literal, Optional, Union

from .common import GenericPath
from .validation import ValidationException

FileOnCloseBehavior = Literal["none", "rm-if-finished"]


@dataclasses.dataclass
class StdioDef:
    path: Optional[GenericPath]
    on_close: FileOnCloseBehavior

    @staticmethod
    def from_path(path: GenericPath) -> "StdioDef":
        return StdioDef(path=path, on_close="none")

    @staticmethod
    def remove_if_finished(path: Optional[GenericPath] = None) -> "StdioDef":
        return StdioDef(path=path, on_close="rm-if-finished")


Stdio = Union[GenericPath, StdioDef]


# Keep in sync with `DEFAULT_STDOUT_PATH`
def default_output(ext: str) -> str:
    return "%{CWD}/%{TASK_ID}." + ext


def default_stdout() -> str:
    return default_output("stdout")


def default_stderr() -> str:
    return default_output("stderr")


# TODO: how to resolve TASK_ID in the context of some other task?
class Output:
    def __init__(self, name: str, filepath: Optional[str] = None, extension: Optional[str] = None):
        if filepath and extension:
            raise ValidationException("Parameters `filepath` and `extension` are mutually exclusive")

        self.name = name
        self.filepath = filepath
        self.extension = extension.lstrip(".")


def generate_name(output: Output) -> str:
    if output.filepath is not None:
        return output.filepath
    extension = f".{output.extension}" or ""
    name = generate_random_name()
    return f"{name}{extension}"


def generate_random_name() -> str:
    return uuid.uuid4()[:16]


def materialize_outputs(collection) -> List[Output]:
    items = []
    if isinstance(collection, (list, tuple)):
        items = collection
    elif isinstance(collection, dict):
        items = collection.values()
    elif isinstance(collection, Output):
        items = [collection]
    return [item for item in items if isinstance(item, Output)]


def gather_outputs(collection) -> List[Output]:
    items = []
    if isinstance(collection, (list, tuple)):
        items = collection
    elif isinstance(collection, dict):
        items = collection.values()
    elif isinstance(collection, Output):
        items = [collection]
    return [item for item in items if isinstance(item, Output)]
