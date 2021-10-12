import enum
from typing import Dict


class AllocationPolicy(enum.Enum):

    Compact = "Compact"
    ForceCompact = "ForceCompact"
    Scatter = "Scatter"


class ResourceRequest:
    def __init__(
        self,
        cpus,
        policy=AllocationPolicy.Compact,
        generic_resources: Dict[str, int] = None,
    ):
        self.cpus = cpus
        self.policy = policy
        self.generic_resources = generic_resources

    def resource_names(self):
        if self.generic_resources is not None:
            return self.generic_resources.keys()
        else:
            return ()

    def to_dict(self, resource_name_map: Dict[str, int]):
        result = {"cpus": {self.policy.value: self.cpus}}
        if self.generic_resources is not None:
            result["generic"] = [
                {"resource": resource_name_map[name], "amount": amount}
                for name, amount in self.generic_resources.items()
            ]
        return result
