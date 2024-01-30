"""
Copy all files from the `examples` directory to `<built-docs>/examples`, so that they can be rendered in the
documentation.
"""
import glob
import os.path

import mkdocs_gen_files

for path in glob.glob("examples/**/*", recursive=True):
    if os.path.isfile(path):
        with open(path) as src_file:
            with mkdocs_gen_files.open(path, "w") as dest_file:
                dest_file.write(src_file.read())
