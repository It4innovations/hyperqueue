import abc
import os
import socket
import subprocess
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


class Slurm(NodeList):
    def resolve(self) -> List[str]:
        return get_slurm_nodes()


def get_slurm_nodes() -> List[str]:
    assert is_inside_slurm()
    output = subprocess.check_output(["scontrol", "show", "hostnames"])
    return output.decode().split("\n")


def is_inside_slurm() -> bool:
    return "SLURM_NODELIST" in os.environ


class Explicit(NodeList):
    def __init__(self, nodes: List[str]):
        self.nodes = nodes

    def resolve(self) -> List[str]:
        return self.nodes


def get_active_nodes() -> NodeList:
    if is_inside_pbs():
        return PBS()
    elif is_inside_slurm():
        return Slurm()
    else:
        return Local()
