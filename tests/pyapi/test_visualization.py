from tempfile import NamedTemporaryFile

from hyperqueue.job import Job
from hyperqueue.visualization import visualize_job

from ..utils.io import check_file_contents


def test_visualization():
    def fn():
        pass

    job = Job()
    a = job.function(fn, name="a")
    b1 = job.function(fn, name="b1", deps=[a])
    b2 = job.function(fn, name="b2", deps=[a])
    c1 = job.function(fn, name="c1", deps=[b1])
    c2 = job.function(fn, name="c2", deps=[b2])
    job.function(fn, name="d", deps=[c1, c2])

    with NamedTemporaryFile() as f:
        visualize_job(job, f.name)
        check_file_contents(
            f.name,
            """digraph job {
a;
b1;
a -> b1;
b2;
a -> b2;
c1;
b1 -> c1;
c2;
b2 -> c2;
d;
c1 -> d;
c2 -> d;
}
""",
        )
