import socket
from contextlib import closing


def check_file_contents(path: str, content):
    assert read_file(path) == str(content)


def read_file(path: str) -> str:
    with open(path) as f:
        return f.read()


def write_file(path: str, content: str):
    with open(path, "w") as f:
        f.write(content)


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]
