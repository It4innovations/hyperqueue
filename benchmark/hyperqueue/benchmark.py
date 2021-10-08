#!/usr/bin/env python3

import pandas as pd
import subprocess as sp
import time

from datetime import datetime


hq_bin = "../../target/debug/hq"
csv_loc = "hq_bench.csv"


def hq_cmd(cmd, measure=True):
    if measure:
        start_time = datetime.now()
        sp.call([hq_bin + ' ' + cmd], shell=True)
        end_time = datetime.now()
        return format(end_time - start_time)
    else:
        sp.call([hq_bin + ' ' + cmd], shell=True)
        return None


def hq_benchmark(cmds, data):
    for cmd in cmds:
        time = hq_cmd(cmd)
        print('Execution of: ' + cmd + '\nTime taken: ' + str(time))
        data['cmd'].append(cmd)
        data['time'].append(time)
    return data


if __name__ == '__main__':
    data = {'cmd': [], 'time': []}

    # Start
    hq_cmd("server start &", measure=False)
    hq_cmd("worker start &", measure=False)
    time.sleep(0.1)

    # Multiple submit bench
    start_time = datetime.now()
    for i in range(100):
        cmd = 'submit sleep 0 --stderr=none'
        hq_cmd(cmd, measure=False)
    hq_cmd('wait 1-100')
    end_time = datetime.now()
    data['cmd'].append('for 1000 ' + cmd)
    data['time'].append(format(end_time - start_time))

    # Simple command bench stack
    cmds = [
        'submit echo "wait" --array=1-10 --stderr=none --stdout=none --wait',
        'submit echo "wait" --array=1-100 --stderr=none --stdout=none --wait',
    ]
    data = hq_benchmark(cmds, data)

    # Stop
    hq_cmd('server stop &')
    df = pd.DataFrame(data)
    df.to_csv(csv_loc)

    # Clean
    sp.call(['rm -rf job-*'], shell=True)
