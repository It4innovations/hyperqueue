"""
Build a complete CLI reference using the https://github.com/spirali/cli_doc tool.
"""

import os
import shutil
import subprocess
import tempfile

from mkdocs.config import Config
from mkdocs.structure.files import File, Files


def is_cli_doc_available() -> bool:
    return shutil.which("cli_doc") is not None


def on_files(files: Files, config: Config):
    if not is_cli_doc_available():
        if os.environ.get("GITHUB_ACTIONS", "0") == "1":
            raise Exception("cli_doc not available in CI")
        print("Skipping generation of CLI reference, because `cli_doc` is not installed")
        return

    hq_path = os.environ.get("HQ_PATH", "target/debug/hq")
    print("Generating CLI reference")

    with tempfile.NamedTemporaryFile() as tmp_file:
        subprocess.run(["cli_doc", hq_path, "--output-filename", tmp_file.name], check=True)
        with open(tmp_file.name) as src:
            content = src.read()
        files.append(File.generated(config, "cli-reference/index.html", content=content))
