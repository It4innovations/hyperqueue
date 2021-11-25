import logging
from pathlib import Path
from typing import List

import typer

from src.build.hq import iterate_binaries
from src.build.repository import TAG_WORKSPACE
from src.clusterutils import ClusterInfo
from src.clusterutils.node_list import Local
from src.environment.hq import HqClusterInfo, HqEnvironment, ProfileMode
from src.workloads import SleepHQ


def compare_tags(tags: List[str]):
    """
    Compares a single benchmark between multiple HyperQueue git tags.
    """
    if TAG_WORKSPACE in tags:
        tags = [TAG_WORKSPACE] + sorted(set(tags) - {TAG_WORKSPACE})
    else:
        tags = sorted(tags)

    info = ClusterInfo(
        workdir=Path("work"),
        node_list=Local(),
        monitor_nodes=True
    )
    results = {}
    for (tag, binary) in zip(tags, iterate_binaries(tags)):
        hq_info = HqClusterInfo(
            cluster=info,
            binary=binary,
            worker_count=1,
            profile_mode=ProfileMode()
        )

        with HqEnvironment(hq_info) as hq_env:
            sleep = SleepHQ()
            ret = sleep.execute(hq_env, 10000)
            results[tag] = ret
    print(results)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    typer.run(compare_tags)
