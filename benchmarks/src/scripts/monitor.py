import logging
import os
import shutil
import sys
import time

import click
import psutil

from cluster.io import measure_and_store


def get_resources():
    cpus = psutil.cpu_percent(percpu=True)
    mem = psutil.virtual_memory().percent
    connections = sum(1 if c[5] == "ESTABLISHED" else 0 for c in psutil.net_connections())
    bytes = psutil.net_io_counters()
    io = psutil.disk_io_counters()

    return {
        "cpu": cpus,
        "mem": mem,
        "connections": connections,
        "net-write": 0 if bytes is None else bytes.bytes_sent,
        "net-read": 0 if bytes is None else bytes.bytes_recv,
        "disk-write": 0 if io is None else io.write_bytes,
        "disk-read": 0 if io is None else io.read_bytes
    }


def generate_record(timestamp):
    resources = get_resources()

    return {
        "timestamp": timestamp,
        "resources": resources,
    }


@click.command()
@click.argument("output")
@click.option("--capture-interval", default=1)
@click.option("--dump-interval", default=10)
def main(output: str, capture_interval: int, dump_interval: int):
    def capture(timestamp):
        try:
            return generate_record(timestamp)
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
