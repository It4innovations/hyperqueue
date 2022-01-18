from typing import List

TaskId = int


class Task:
    def __init__(self, dependencies: List["Task"] = None):
        self.dependencies = dependencies or ()
