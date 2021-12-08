from pathlib import Path
from typing import Any, Dict


class Environment:
    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class EnvironmentDescriptor:
    """
    This class should describe an instance of an environment.
    The class has to be easily picklable and able to create new environments.
    It also has to be able to describe itself using metadata.
    """

    def create_environment(self, workdir: Path) -> Environment:
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def parameters(self) -> Dict[str, Any]:
        raise NotImplementedError

    def metadata(self) -> Dict[str, Any]:
        return {}
