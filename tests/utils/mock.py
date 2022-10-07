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

    def redirect_program_to_binary(self, mocked_program: str, target_binary: Path):
        """
        Mocks `mocked_program` so that when you try to execute it, `target_binary` will be executed
        instead.
        """
        link_path = self.directory / mocked_program
        link_path.unlink(missing_ok=True)
        os.symlink(dst=link_path, src=target_binary)
        # Make the link executable
        os.chmod(link_path, 0o700)

    @contextlib.contextmanager
    def mock_program_with_code(self, name: str, code: str):
        content = f"#!{sys.executable}\n{code}"
        program_path = self.directory / name
        assert not program_path.is_file()

        with open(program_path, "w") as f:
            f.write(content)
        os.chmod(program_path, 0o700)

        yield

        os.unlink(program_path)
