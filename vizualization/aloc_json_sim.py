import dataclasses
import json
import os
import random
import time
from datetime import datetime
import multiprocessing as mp
from tqdm import tqdm


@dataclasses.dataclass
class WorkerConnected:
    extra: dict
    id: int
    type: str = "worker-connected"


@dataclasses.dataclass
class WorkerOverview:
    hw_state: dict
    id: int
    type: str = "worker-overview"


@dataclasses.dataclass
class AlocationQued:
    id: int
    worker_count: int
    type: str = "autoalloc-allocation-qued"


@dataclasses.dataclass
class AllocationStarted:
    id: int
    type: str = "autoalloc-allocation-started"


@dataclasses.dataclass
class AllocationFinished:
    id: int
    type: str = "autoalloc-allocation-finished"


def event_builder(Alloc):
    time_now = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S.%fZ')
    return dict(event=dataclasses.asdict(Alloc), time=time_now)


WorkerId = 1


def alloc_start(id, WorkerId):
    output = []
    worker_count = random.randrange(1, 10, 1)
    # qued
    qued = AlocationQued(id, worker_count)
    output.append(event_builder(qued))

    time.sleep(random.randrange(0, 60, 1))
    # start
    start = AllocationStarted(id)
    output.append(event_builder(start))

    # create workers
    for i in range(worker_count):
        output.append(event_builder(WorkerConnected(dict(auto_allo_que=id), WorkerId)))
        WorkerId += 1

    alloc_run = random.randrange(1, 60, 1)
    print(f"{id} worker loop")
    for i in range(alloc_run):
        for w_id in range(WorkerId - worker_count, WorkerId, 1):
            cpus = []
            for z in range(8):
                cpus.append(random.randrange(0, 100, 1))
            output.append(
                event_builder(WorkerOverview(dict(worker_cpu_usage=dict(cpu_per_core_percent_usage=cpus)), w_id)))


        time.sleep(2)
    print(f"{id} finished")
    output.append(event_builder(AllocationFinished(id)))
    return output, WorkerId


QueueCount = 4
processes = []
events = []
for i in range(QueueCount):
    print("Aloc start")
    event, worker_id = alloc_start(i, WorkerId)
    events.extend(event)
    WorkerId = worker_id

# while True:
#     state = []
#     for p in processes:
#         if p.is_alive():
#             events.extend(parent.recv())
#             print(len(events))
#             state.append(False)
#         else:
#             state.append(True)
#
#     if not False in state:
#         break

with open('data.json', 'w') as fp:
    for event in events:
        fp.write(f"{json.dumps(event)}\n")

print("finished")
