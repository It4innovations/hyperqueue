import pytest

from .mock.command import CommandHandler
from .mock.manager import DefaultManager, Manager
from .mock.pbs import PbsCommandHandler
from .mock.slurm import SlurmCommandHandler
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

    def adapt(self, manager: Manager) -> CommandHandler:
        raise NotImplementedError

    def default_handler(self) -> CommandHandler:
        return self.adapt(DefaultManager())


class PbsManagerFlavor(ManagerFlavor):
    def manager_type(self) -> ManagerType:
        return "pbs"

    def submit_program_name(self) -> str:
        return "qsub"

    def adapt(self, manager: Manager) -> CommandHandler:
        return PbsCommandHandler(manager)


class SlurmManagerFlavor(ManagerFlavor):
    def manager_type(self) -> ManagerType:
        return "slurm"

    def submit_program_name(self) -> str:
        return "sbatch"

    def adapt(self, manager: Manager) -> CommandHandler:
        return SlurmCommandHandler(manager)


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
