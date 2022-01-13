import json
import subprocess


def latest_version():
    return {"type": "latest"}


def get_version(output: str):
    if not output:
        return latest_version()
    else:
        tags = [
            t.strip() for t in output.splitlines(keepends=False) if t.startswith("v")
        ]
        if not tags:
            return latest_version()
        return {"type": "stable", "version": sorted(tags, reverse=True)[0]}


if __name__ == "__main__":
    """
    Calculates whether the current commit is a stable version (=there is some tag pointing to it) or
    an unstable one.
    """
    output = (
        subprocess.check_output(["git", "tag", "--points-at", "HEAD"]).decode().strip()
    )
    version = get_version(output)
    print(json.dumps(version))
