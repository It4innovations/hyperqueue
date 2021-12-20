import dataclasses
from pathlib import Path
from typing import List

from ..utils import is_binary_available

PROFILER_METADATA_KEY = "profiler"


@dataclasses.dataclass
class ProfiledCommand:
    args: List[str]
    tag: str
    output_path: Path


class Profiler:
    def check_availability(self):
        raise NotImplementedError

    def profile(self, command: List[str], output_dir: Path) -> ProfiledCommand:
        raise NotImplementedError


class FlamegraphProfiler(Profiler):
    TAG = "flamegraph"

    def __init__(self, frequency: int):
        self.frequency = frequency

    def check_availability(self):
        if not is_binary_available("flamegraph"):
            raise Exception(
                """Flamegraph profiling is not available.
Please install cargo-flamegraph: `cargo install flamegraph` and make sure that `perf` is available.
""".strip()
            )

    def profile(self, command: List[str], output_dir: Path) -> ProfiledCommand:
        path = output_dir / "flamegraph.svg"

        args = [
            "flamegraph",
            "-o",
            str(path),
            "--freq",
            str(self.frequency),
            "--",
        ] + command
        return ProfiledCommand(args=args, tag=FlamegraphProfiler.TAG, output_path=path)

    def __repr__(self):
        return "FlamegraphProfiler"


class PerfEventsProfiler(Profiler):
    TAG = "perf-events"

    def __init__(self, events: List[str] = None):
        if events is None:
            events = ["cache-misses", "branch-misses", "cycles", "instructions"]
        assert events
        self.events = events

    def check_availability(self):
        if not is_binary_available("perf"):
            raise Exception(
                "Performance events profiling is not available. Please install `perf`."
            )

    def profile(self, command: List[str], output_dir: Path) -> ProfiledCommand:
        path = output_dir / "perf-events.txt"
        args = [
            "perf",
            "stat",
            "-o",
            str(path),
            "-e",
            ",".join(self.events),
            "--",
        ] + command

        return ProfiledCommand(args=args, tag=PerfEventsProfiler.TAG, output_path=path)

    def __repr__(self):
        return "PerfEventsProfiler"
