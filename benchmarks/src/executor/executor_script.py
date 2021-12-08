import json
import pickle
import sys

from ..utils import activate_cwd
from .local_executor import execute_benchmark
from .serialization import SerializedBenchmark, serialize_result

if __name__ == "__main__":
    pipe_path = sys.argv[1]

    data = sys.stdin.buffer.read()
    benchmark = pickle.loads(data)
    assert isinstance(benchmark, SerializedBenchmark)

    with activate_cwd(benchmark.cwd):
        result = execute_benchmark(benchmark.descriptor, benchmark.ctx)
        serialized_result = serialize_result(result)
        with open(pipe_path, "w") as file:
            print(json.dumps(serialized_result), file=file)
