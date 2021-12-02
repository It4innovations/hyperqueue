from pathlib import Path
from typing import List

from ..utils import is_binary_available


class NativeProfiler:
    def check_availability(self):
        if not is_binary_available("flamegraph"):
            raise Exception(
                """Native flamegraph profiling is not available.
Please install cargo-flamegraph: `cargo install flamegraph` and make sure that `perf` is available.
""".strip()
            )

    def profile(
        self, command: List[str], output_file: Path, frequency: int
    ) -> List[str]:
        return [
            "flamegraph",
            "-o",
            str(output_file),
            "--freq",
            str(frequency),
            "--",
        ] + command

    def __repr__(self):
        return "NativeProfiler"
