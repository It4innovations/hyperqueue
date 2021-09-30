import contextlib
import os
import sys
from os.path import isfile, join
from typing import Dict


class ProgramMock:
    def __init__(self, directory: str):
        self.directory = str(os.path.abspath(directory))
        os.makedirs(self.directory, exist_ok=True)

    def update_env(self, env: Dict[str, str]):
        path = self.directory
        if "PATH" in env:
            path += f":{env['PATH']}"
        env["PATH"] = path

    @contextlib.contextmanager
    def mock_program(self, name: str, code: str):
        content = f"#!{sys.executable}\n{code}"
        program_path = join(self.directory, name)
        assert not isfile(program_path)

        with open(program_path, "w") as f:
            f.write(content)
        os.chmod(program_path, 0o700)

        yield

        os.unlink(program_path)
