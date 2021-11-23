import abc
import os
import socket
from typing import List


class NodeList(abc.ABC):
    def resolve(self) -> List[str]:
        raise NotImplementedError()

    def is_localhost(self) -> bool:
        return False


class Local(NodeList):
    HOSTNAME = socket.gethostname()

    def resolve(self) -> List[str]:
        return [Local.HOSTNAME]

    def is_localhost(self) -> bool:
        return True


class PBS(NodeList):
    def resolve(self) -> List[str]:
        return get_pbs_nodes()


def is_inside_pbs() -> bool:
    return "PBS_NODEFILE" in os.environ


def get_pbs_nodes() -> List[str]:
    assert is_inside_pbs()

    with open(os.environ["PBS_NODEFILE"]) as f:
        return [line.strip() for line in f]


class Explicit(NodeList):
    def __init__(self, nodes: List[str]):
        self.nodes = nodes

    def resolve(self) -> List[str]:
        return self.nodes
