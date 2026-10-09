"""Run one scenario on Ray (heterogeneous fake nodes on one machine).

usage: run_ray.py ARM SCENARIO OUTPUT_JSON [DIV]
Scenarios with resource variants are not supported (Ray has no variants).
"""

import json
import sys
import time

import ray
from ray.cluster_utils import Cluster

import workload


@ray.remote
def task(tid, duration):
    s = time.time()
    time.sleep(duration)
    e = time.time()
    return tid, ray.get_runtime_context().get_node_id(), s, e


def main():
    arm, name, output = sys.argv[1:4]
    div = float(sys.argv[4]) if len(sys.argv) > 4 else workload.DIV
    sc = workload.scenario(name, div)
    assert not sc.uses_variants, "Ray has no resource variants"

    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 0, "num_gpus": 0,
                                                            "include_dashboard": False})
    labels = {}
    late = []

    def add(label, cpus, gpus):
        node = cluster.add_node(num_cpus=cpus, num_gpus=gpus)
        labels[node.node_id] = label

    for label, ntype, cpus, gpus in workload.nodes():
        if ntype in sc.late_nodes:
            late.append((sc.late_nodes[ntype], label, cpus, gpus))
        else:
            add(label, cpus, gpus)
    ray.init(address=cluster.address, log_to_driver=False)
    cluster.wait_for_nodes()
    # warm-up: fill every CPU slot of every node at once, so that Ray starts all its
    # worker processes before the measurement
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    warm = []
    for node_id, label in labels.items():
        cpus = next(c for lab, _, c, _ in workload.nodes() if lab == label)
        strategy = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
        warm += [task.options(num_cpus=1, scheduling_strategy=strategy).remote(-1, 1.0)
                 for _ in range(cpus)]
    ray.get(warm)

    t0 = time.time()
    refs = []
    for t in sc.tasks:
        c, g, d = t.variants[0]
        refs.append(task.options(num_cpus=c, num_gpus=g).remote(t.tid, d))
    t_submit = time.time()
    late.sort()
    pending = refs
    while pending:
        if late and time.time() >= t0 + late[0][0]:
            _, label, cpus, gpus = late.pop(0)
            add(label, cpus, gpus)
        _, pending = ray.wait(pending, num_returns=len(pending), timeout=0.05)
    t_end = time.time()
    results = ray.get(refs)
    recs = [[tid, 0, labels.get(node, node), s, e] for tid, node, s, e in results]
    ray.shutdown()
    cluster.shutdown()

    out = {"arm": arm, "scenario": name, "div": div, "t0": t0, "t_submit": t_submit,
           "t_end": t_end, "records": recs}
    with open(output, "w") as f:
        json.dump(out, f)
    print(f"{arm} {name}: tasks={len(recs)}/{len(sc.tasks)} "
          f"makespan={max(r[4] for r in recs) - t0:.2f}s submit={t_submit - t0:.3f}s")


if __name__ == "__main__":
    main()
