from collections import defaultdict
from typing import Dict, List, Optional, Protocol


class Init:
    pass


class Started:
    pass


class Stopped:
    pass


class EnvStateManager:
    """
    Helper mixin class that makes sure that an environment is used in the correct order and that
    it is not started/stopped multiple times.
    """

    def __init__(self):
        self.state = Init()

    def state_start(self):
        assert isinstance(self.state, Init)
        self.state = Started()

    def state_stop(self):
        assert isinstance(self.state, Started)
        self.state = Stopped()


def sanity_check_nodes(nodes: List[str]):
    for node in nodes:
        assert len(node) > 0
    assert len(set(nodes)) == len(nodes)
    assert len(nodes) > 0


class WorkerConfig(Protocol):
    node: Optional[int]


def assign_workers(
    workers: List[WorkerConfig], nodes: List[str]
) -> Dict[str, List[WorkerConfig]]:
    round_robin_node = 0
    used_round_robin = set()

    node_assignments = defaultdict(list)
    for index, worker in enumerate(workers):
        node = worker.node
        if node is not None:
            if not (0 <= node < len(nodes)):
                raise Exception(
                    f"Invalid node assignment. Worker {index} wants to be on node "
                    f"{node}, but there are only {len(nodes)} worker nodes"
                )
        else:
            node = round_robin_node
            round_robin_node = (round_robin_node + 1) % len(nodes)
            if node in used_round_robin:
                raise Exception(
                    f"There are more workers ({len(workers)}) than worker nodes ({len(nodes)})"
                )
            used_round_robin.add(node)
        if node >= len(nodes):
            raise Exception(
                f"Selected worker node is {node}, but there are only {len(nodes)} worker node(s)"
            )
        node_assignments[nodes[node]].append(worker)
    return dict(node_assignments)
