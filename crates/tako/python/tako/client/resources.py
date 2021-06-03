class ResourceRequest:
    def __init__(self, cpus):
        self.cpus = cpus

    def to_dict(self):
        return {"cpus": {"Compact": self.cpus}}
