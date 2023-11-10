import logging
import os
import shutil
import sys
import time

import click
import psutil
from cluster.io import measure_and_store
from record import generate_record, MonitoringOptions


@click.command()
@click.argument("output")
@click.option("--capture-interval", default=1)
@click.option("--dump-interval", default=10)
@click.option("--observe-pids", default="")
def main(output: str, capture_interval: int, dump_interval: int, observe_pids: str):
    options = MonitoringOptions(observe_network=False)

    processes = []
    process_map = {}
    for pid in observe_pids.split(","):
        try:
            processes.append(psutil.Process(int(pid)))
            logging.info(f"Observing PID {pid}")
        except BaseException as e:
            logging.error(e)

    def capture(timestamp):
        try:
            start = time.time()
            result = generate_record(timestamp, processes, process_map, options)
            duration = time.time() - start
            logging.info(f"Capturing data took {duration:.5f}s")
            return result
        except Exception as e:
            logging.error("Opening cluster exception: {}".format(e))
            return None

    def finish():
        logging.info(f"Copying trace from {tmp_output} to {output}")
        shutil.copyfile(tmp_output, output)
        sys.exit()

    tmp_output = f"/tmp/{os.path.basename(output)}-{int(time.time())}"

    # Create temporary file
    with open(tmp_output, "w") as _:
        pass

    measure_and_store(capture_interval, dump_interval, tmp_output, capture, finish)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
