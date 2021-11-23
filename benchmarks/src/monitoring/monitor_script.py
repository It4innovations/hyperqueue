import logging
import os
import shutil
import sys
import time

import click
import psutil
from cluster.io import measure_and_store

from record import generate_record


@click.command()
@click.argument("output")
@click.option("--capture-interval", default=1)
@click.option("--dump-interval", default=10)
@click.option("--observe-pids", default="")
def main(output: str, capture_interval: int, dump_interval: int, observe_pids: str):
    processes = []
    for pid in observe_pids.split(","):
        try:
            processes.append(psutil.Process(int(pid)))
            logging.info(f"Observing PID {pid}")
        except BaseException as e:
            logging.error(e)

    def capture(timestamp):
        try:
            return generate_record(timestamp, processes)
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
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s:%(asctime)s:%(funcName)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    main()
