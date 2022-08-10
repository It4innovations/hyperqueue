from .common import GenericPath
from .job import Job
from .task.task import Task
from .utils.package import MissingPackageException


def visualize_job(job: Job, path: GenericPath):
    """
    Visualizes the task graph of the passed job in the DOT format.
    The result is written to a file located at `path`.

    Note: this function requires the `pydot` package to be installed.
    """

    try:
        import pydot
    except ImportError:
        raise MissingPackageException("pydot")

    graph = pydot.Dot("job", graph_type="digraph")
    visited = {}

    def visit(task: Task):
        nonlocal visited, graph
        if task.task_id in visited:
            return visited[task.task_id]

        node = pydot.Node(task.label)
        graph.add_node(node)
        for dep in task.dependencies:
            dep_node = visit(dep)
            edge = pydot.Edge(dep_node.get_name(), node.get_name())
            graph.add_edge(edge)

        visited[task.task_id] = node
        return node

    for task in job.tasks:
        visit(task)

    graph.write(path)
