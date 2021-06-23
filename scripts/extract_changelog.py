import sys
from os.path import dirname, abspath, join


CURRENT_DIR = dirname(abspath(__file__))
CHANGELOG_PATH = join(dirname(CURRENT_DIR), "CHANGELOG.md")


def normalize(version: str) -> str:
    return version.strip().lstrip("v").lower()


def get_matching_lines(text: str, tag: str):
    lines = list(text.splitlines(keepends=False))
    for (index, line) in enumerate(lines):
        if line.startswith("# "):
            version = normalize(line.lstrip("# "))
            if version == tag:
                for matching_line in lines[index + 1:]:
                    if matching_line.startswith("# "):
                        return
                    yield matching_line


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_changelog <tag>")
        exit(1)

    tag = normalize(sys.argv[1])
    with open(CHANGELOG_PATH) as f:
        text = f.read()

    output = f"# HyperQueue {tag}\n"
    for line in get_matching_lines(text, tag):
        output += f"{line}\n"
    print(output)
