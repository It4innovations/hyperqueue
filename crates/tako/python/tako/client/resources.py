import enum


class AllocationPolicy(enum.Enum):

    Compact = "Compact"
    ForceCompact = "ForceCompact"
    Scatter = "Scatter"


class ResourceRequest:
    def __init__(self, cpus, policy=AllocationPolicy.Compact):
        self.cpus = cpus
        self.policy = policy

    def to_dict(self):
        return {"cpus": {self.policy.value: self.cpus}}
