"""
Build a complete CLI reference using the https://github.com/spirali/cli_doc tool.
"""

import os
import shutil
import subprocess
import tempfile

import mkdocs_gen_files


def is_cli_doc_available() -> bool:
    return shutil.which("cli_doc") is not None


def run():
    if not is_cli_doc_available():
        if os.environ.get("GITHUB_ACTIONS", "0") == "1":
            raise Exception("cli_doc not available in CI")
        print("Skipping generation of CLI reference, because `cli_doc` is not installed")
        return

    hq_path = os.environ.get("HQ_PATH", "target/debug/hq")
    print("Generating CLI reference")

    with tempfile.NamedTemporaryFile() as tmp_file:
        subprocess.run(["cli_doc", hq_path, "--output-filename", tmp_file.name], check=True)
        with mkdocs_gen_files.open("cli-reference/index.html", "w") as dst:
            with open(tmp_file.name) as src:
                dst.write(src.read())


run()
