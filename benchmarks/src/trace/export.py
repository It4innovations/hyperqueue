import datetime
import enum
import json
from collections import defaultdict
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Any

import dataclasses


class EventType(enum.Enum):
    DurationBegin = "B"
    DurationEnd = "E"


@dataclasses.dataclass(frozen=True)
class Event:
    name: str
    pid: str
    type: EventType
    timestamp: datetime.timedelta
    resources: List[str] = dataclasses.field(default_factory=list)
    categories: List[str] = dataclasses.field(default_factory=list)

    @staticmethod
    def duration_begin(
        name: str, worker: str, resources: List[str], timestamp: datetime.timedelta
    ) -> "Event":
        return Event(
            name=name,
            pid=worker,
            type=EventType.DurationBegin,
            resources=resources,
            timestamp=timestamp,
        )

    @staticmethod
    def duration_end(
        name: str, worker: str, resources: List[str], timestamp: datetime.timedelta
    ) -> "Event":
        return Event(
            name=name,
            pid=worker,
            type=EventType.DurationEnd,
            resources=resources,
            timestamp=timestamp,
        )

    def serialize(self) -> List[Dict[str, Any]]:
        return [
            dict(
                name=self.name,
                ph=self.type.value,
                cat=",".join(self.categories),
                ts=int(self.timestamp.total_seconds() * 1_000_000),
                tid=res,
                pid=self.pid,
            )
            for res in self.resources
        ]


def export_hq_events_to_chrome(event_path: Path, chrome_output: Path):
    events = []
    for event in load_events(event_path):
        for serialized in event.serialize():
            events.append(serialized)
    with open(chrome_output, "w") as f:
        json.dump(events, f)


def load_events(path: Path) -> Iterable[Event]:
    exporter = WorkerTaskExport()

    with open(path) as f:
        for line in f:
            event = json.loads(line)
            exported = exporter.export(event)
            if exported is not None:
                yield exported


def parse_hq_time(time: str) -> datetime.datetime:
    # Skip timezone
    if time.endswith("Z"):
        time = time[:-1]
    # Skip nanoseconds
    if len(time.split(".")[-1]) == 9:
        time = time[:-3]
    return datetime.datetime.fromisoformat(time)


class WorkerTaskExport:
    def __init__(self):
        self.task_to_worker: Dict[int, int] = {}
        self.worker_to_task: Dict[int, Dict[int, int]] = defaultdict(dict)
        self.first_timestamp: Optional[datetime.datetime] = None

    def export(self, event) -> Optional[Event]:
        # Skip nanoseconds and trailing Z
        time = parse_hq_time(event["time"][:-4])
        if self.first_timestamp is None:
            ts_from_start = datetime.timedelta()
            self.first_timestamp = time
        else:
            ts_from_start = time - self.first_timestamp
        payload = event["event"]
        type = payload["type"]

        if type == "task-started":
            task_id = payload["id"]
            worker = payload["worker"]
            # assert task_id not in self.task_to_worker
            self.task_to_worker[task_id] = worker
            tid = 0
            tids = frozenset(self.worker_to_task[worker].values())
            while True:
                if tid not in tids:
                    break
                tid += 1
            self.worker_to_task[worker][task_id] = tid

            resources = [str(tid)]
            return Event.duration_begin(
                name=f"Task {task_id}",
                worker=str(worker),
                resources=resources,
                timestamp=ts_from_start,
            )
        elif type == "task-finished":
            task_id = payload["id"]
            assert task_id in self.task_to_worker
            worker = self.task_to_worker.pop(task_id)
            tid = self.worker_to_task[worker].pop(task_id)
            resources = [str(tid)]
            return Event.duration_end(
                name=f"Task {task_id}",
                worker=str(worker),
                resources=resources,
                timestamp=ts_from_start,
            )
        return None
