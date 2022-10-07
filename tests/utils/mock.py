import contextlib
import os
import sys
from pathlib import Path
from typing import Dict


class ProgramMock:
    def __init__(self, directory: str):
        self.directory = Path(os.path.abspath(directory))
        os.makedirs(self.directory, exist_ok=True)

    def update_env(self, env: Dict[str, str]):
        path = str(self.directory)
        if "PATH" in env:
            path += f":{env['PATH']}"
        env["PATH"] = path

    def mock(self, name: str, target: Path):
        link_path = self.directory / name
        link_path.unlink(missing_ok=True)
        os.symlink(dst=link_path, src=target)
        # Make the link executable
        os.chmod(link_path, 0o700)

    @contextlib.contextmanager
    def mock_program(self, name: str, code: str):
        content = f"#!{sys.executable}\n{code}"
        program_path = self.directory / name
        assert not program_path.is_file()

        with open(program_path, "w") as f:
            f.write(content)
        os.chmod(program_path, 0o700)

        yield

        os.unlink(program_path)
