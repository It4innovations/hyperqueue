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


class CachegrindProfiler(Profiler):
    TAG = "cache-grind"

    def __init__(self):
        pass

    def check_availability(self):
        if not is_binary_available("valgrind"):
            raise Exception(
                "Valgrind profiling is not available. Please install `valgrind`."
            )

    def profile(self, command: List[str], output_dir: Path) -> ProfiledCommand:
        path = output_dir / "cachegrind.txt"
        args = [
            "valgrind",
            "--tool=cachegrind",
            "--log-file="+str(path),
        ] + command
        return ProfiledCommand(args=args, tag=CachegrindProfiler.TAG, output_path=path)

    def __repr__(self):
        return "CachegrindProfiler"


# Process into flamegraphs
# 1. Apply profiling records
# 2. perf script -i perf-records.txt | inferno-collapse-perf > stacks.folded
# (Optional: Vizualize) cat stacks.folded | inferno-flamegraph > profile.svg
# (Optional: Compare) inferno-diff-folded stacks1.folded stacks2.folded | inferno-flamegraph > flamediff.svg
class PerfRecordsProfiler(Profiler):
    TAG = "perf-records"

    def __init__(self):
        pass

    def check_availability(self):
        if not is_binary_available("perf"):
            raise Exception(
                "Performance records profiling is not available. Please install `perf`."
            )

    def profile(self, command: List[str], output_dir: Path) -> ProfiledCommand:
        path = output_dir / "perf-records.txt"

        args = [
            "perf",
            "record",
            "-o",
            str(path)
        ] + command

        return ProfiledCommand(args=args, tag=PerfRecordsProfiler.TAG, output_path=path)

    def __repr__(self):
        return "PerfRecordsProfiler"
