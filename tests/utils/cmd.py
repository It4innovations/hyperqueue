from typing import List


def python(command: str) -> List[str]:
    """
    Returns commands that will run the specified command as a Python script.
    """
    return ["python3", "-c", command]


def bash(command: str) -> List[str]:
    """
    Returns commands that will run in a bash script.
    """
    return ["bash", "-c", command]
