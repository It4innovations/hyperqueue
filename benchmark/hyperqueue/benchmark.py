import subprocess as sp
import time

from datetime import datetime


hq_bin = "../target/release/hq "


def hq_cmd(cmd, samples=1):
    start_time = datetime.now()
    for sample in range(samples):
        sp.call([hq_bin + cmd], shell=True)
    sp.call([hq_bin + f"wait 1-{samples}"], shell=True)
    end_time = datetime.now()
    return (end_time - start_time).total_seconds()


def benchmark(cmd, samples, data):
    # Start
    sp.call([hq_bin + "server start &"], shell=True)
    time.sleep(0.1)
    sp.call([hq_bin + "worker start &"], shell=True)
    time.sleep(0.1)

    time_taken = hq_cmd(cmd, samples)
    print(f"Execution of {cmd}\nTime taken: {time_taken}")
    data['cmd'].append(cmd)
    data['time'].append(time_taken)
    data['system'].append('hyperqueue')

    # Stop
    sp.call([hq_bin + "server stop &"], shell=True)
    return data
