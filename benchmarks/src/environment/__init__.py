from typing import Any, Dict


class Environment:
    def name(self) -> str:
        raise NotImplementedError()

    def create_environment_key(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def create_metadata_key(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()
