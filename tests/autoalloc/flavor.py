import pytest

from .mock.command import CommandHandler
from .mock.manager import DefaultManager, Manager
from .mock.pbs import PbsCommandHandler
from .mock.slurm import adapt_slurm

from .utils import ManagerType


class ManagerFlavor:
    """
    Represents a specific flavor of a manager (PBS/Slurm).
    It should be able to `adapt` any manager to an interface used by that flavor.
    """

    def manager_type(self) -> ManagerType:
        raise NotImplementedError

    def adapt(self, manager: Manager) -> CommandHandler:
        raise NotImplementedError

    def default_handler(self) -> CommandHandler:
        return self.adapt(DefaultManager())


class PbsManagerFlavor(ManagerFlavor):
    def manager_type(self) -> ManagerType:
        return "pbs"

    def adapt(self, manager: Manager) -> CommandHandler:
        return PbsCommandHandler(manager)


class SlurmManagerFlavor(ManagerFlavor):
    def manager_type(self) -> ManagerType:
        return "slurm"

    def adapt(self, manager: Manager) -> CommandHandler:
        return adapt_slurm(manager)


def all_flavors(fn):
    """
    Test fixture that generates all available manager flavors.
    """
    return pytest.mark.parametrize(
        "flavor",
        (
            # SlurmManagerFlavor(),
            PbsManagerFlavor(),
        ),
        # ids=["slurm", "pbs"],
        ids=["pbs"],
    )(fn)
