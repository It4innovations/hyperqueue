import logging
import traceback
from pathlib import Path
from typing import Iterable, List, Tuple

from ..executor.executor import BenchmarkContext, BenchmarkExecutor
from ..executor.external_executor import ExternalBenchmarkExecutor
from ..executor.local_executor import LocalBenchmarkExecutor
from .database import BenchmarkResultRecord, Database
from .identifier import (
    BenchmarkDescriptor,
    BenchmarkIdentifier,
    BenchmarkInstance,
    create_identifiers,
)
from .result import BenchmarkResult, Failure, Success, Timeout

DEFAULT_TIMEOUT_S = 120


class BenchmarkRunner:
    def __init__(self, database: Database, workdir: Path, exit_on_error=True):
        self.database = database
        self.workdir = workdir.resolve()
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.exit_on_error = exit_on_error

    def materialize_and_skip(self, descriptors: List[BenchmarkDescriptor]) -> List[BenchmarkInstance]:
        instances = create_identifiers(descriptors, workdir=self.workdir, default_timeout_s=DEFAULT_TIMEOUT_S)
        return self._skip_completed(instances)

    def compute_materialized(
        self, instances: List[BenchmarkInstance]
    ) -> Iterable[Tuple[BenchmarkInstance, BenchmarkResult]]:
        for instance in instances:
            identifier = instance.identifier

            logging.info(f"Executing benchmark {identifier}")
            ctx = BenchmarkContext(workdir=Path(identifier.workdir), timeout_s=identifier.timeout)
            executor = self._create_executor(instance.descriptor)

            try:
                result = executor.execute(instance.descriptor, ctx=ctx)
            except BaseException:
                tb = traceback.format_exc()
                logging.error(f"Unexpected benchmarking error has occurred: {tb}")
                result = Failure(tb)

            self._handle_result(identifier, result)

            yield (instance, result)

    def compute(self, descriptors: List[BenchmarkDescriptor]) -> Iterable[Tuple[BenchmarkInstance, BenchmarkResult]]:
        instances = self.materialize_and_skip(descriptors)
        yield from self.compute_materialized(instances)

    def save(self):
        self.database.save()

    def _skip_completed(self, infos: List[BenchmarkInstance]) -> List[BenchmarkInstance]:
        not_completed = []
        visited = set()
        skipped = 0

        for info in infos:
            key = info.identifier.key
            if key in visited:
                raise Exception(f"Duplicated identifier: {info.identifier} in {infos}")
            visited.add(key)

            if not self.database.has_record_for(info.identifier):
                not_completed.append(info)
            else:
                skipped += 1

        total_count = skipped + len(not_completed)
        logging.info(f"Skipping {skipped} out of {total_count} benchmark(s)")
        return not_completed

    def _handle_result(self, identifier: BenchmarkIdentifier, result: BenchmarkResult):
        key = identifier
        duration = None

        if isinstance(result, Failure):
            logging.error(f"Benchmark {key} has failed: {result.traceback}")
            if self.exit_on_error:
                raise Exception(f"""Benchmark {identifier} has failed: {result}
You can find details in {identifier.workdir}""")
        elif isinstance(result, Timeout):
            logging.info(f"Benchmark {key} has timeouted after {result.timeout}s")
        elif isinstance(result, Success):
            logging.info(f"Benchmark {key} has finished in {result.duration}s")
            duration = result.duration
        else:
            raise Exception(f"Unknown benchmark result {result}")

        self.database.store(identifier, BenchmarkResultRecord(duration=duration))

    def _create_executor(self, descriptor: BenchmarkDescriptor) -> BenchmarkExecutor:
        if descriptor.executor_config is not None:
            return ExternalBenchmarkExecutor(descriptor.executor_config.init_script)
        return LocalBenchmarkExecutor()
