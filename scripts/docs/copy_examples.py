"""
Copy all files from the `examples` directory to `<built-docs>/examples`, so that they can be rendered in the
documentation.
"""

import glob
import os.path

from mkdocs.config import Config
from mkdocs.structure.files import File, Files


def on_files(files: Files, config: Config):
    file_count = 0
    for path in glob.glob("examples/**/*", recursive=True):
        if os.path.isfile(path):
            with open(path) as src_file:
                content = src_file.read()

            file_count += 1
            files.append(File.generated(config, path, content=content))
    print(f"Copied {file_count} files from the examples directory")
