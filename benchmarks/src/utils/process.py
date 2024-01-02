import subprocess
from pathlib import Path
from typing import Dict, List, Optional


def execute_process(
    args: List[str],
    stdout: Path,
    stderr: Path,
    env: Optional[Dict[str, str]] = None,
    check=True,
) -> subprocess.CompletedProcess:
    with open(stdout, "wb") as stdout_file:
        with open(stderr, "wb") as stderr_file:
            env = env or {}
            result = subprocess.run(
                args,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
            )
    if check:
        if result.returncode != 0:
            with open(stdout) as stdout_file:
                with open(stderr) as stderr_file:
                    raise Exception(
                        f"""The process {args} has exited with error code {result.returncode}
Stdout: {stdout_file.read()}
Stderr: {stderr_file.read()}
""".strip()
                    )
    return result
