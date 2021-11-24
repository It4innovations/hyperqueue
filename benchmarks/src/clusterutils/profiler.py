import shutil
from abc import ABC
from pathlib import Path
from typing import List


class Profiler(ABC):
    def is_available(self) -> bool:
        return True

    def profile(self, command: List[str], output_file: Path, frequency: int) -> List[str]:
        return command


class NativeProfiler:
    def is_available(self) -> bool:
        return shutil.which("flamegraph") is not None

    def profile(self, command: List[str], output_file: Path, frequency: int) -> List[str]:
        return ["flamegraph", "-o", str(output_file), "--freq", str(frequency), "--"] + command

    def __repr__(self):
        return "NativeProfiler"
