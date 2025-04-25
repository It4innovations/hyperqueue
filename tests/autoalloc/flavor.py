import pytest

from .mock.manager import ManagerAdapter, CommandHandler
from .mock.pbs import PbsAdapter
from .mock.slurm import SlurmAdapter
from .utils import ManagerType


class ManagerFlavor:
    """
    Represents a specific flavor of a manager (PBS/Slurm).
    It should be able to `adapt` any manager to an interface used by that flavor.
    """

    def manager_type(self) -> ManagerType:
        raise NotImplementedError

    def submit_program_name(self) -> str:
        raise NotImplementedError

    def create_adapter(self) -> ManagerAdapter:
        raise NotImplementedError

    def default_handler(self) -> CommandHandler:
        return CommandHandler(self.create_adapter())


class PbsManagerFlavor(ManagerFlavor):
    def manager_type(self) -> ManagerType:
        return "pbs"

    def submit_program_name(self) -> str:
        return "qsub"

    def create_adapter(self) -> ManagerAdapter:
        return PbsAdapter()


class SlurmManagerFlavor(ManagerFlavor):
    def manager_type(self) -> ManagerType:
        return "slurm"

    def submit_program_name(self) -> str:
        return "sbatch"

    def create_adapter(self) -> ManagerAdapter:
        return SlurmAdapter()


def all_flavors(fn):
    """
    Test fixture that generates all available manager flavors.
    """
    return pytest.mark.parametrize(
        "flavor",
        (
            SlurmManagerFlavor(),
            PbsManagerFlavor(),
        ),
        ids=["slurm", "pbs"],
    )(fn)
