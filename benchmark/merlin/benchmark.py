#!/usr/bin/env python3

import pandas as pd
import subprocess as sp
import time

from datetime import datetime


csv_loc = "merlin_bench.csv"


def merlin_cmd(cmd, samples):
    start_time = datetime.now()
    merlin_file = open("bench.yaml", "w")

    merlin_file.writelines(
        'description:\n'
        '   name: Merlin Benchmark\n'
        '   description: Simple bench\n\n'
    )

    # Create merlin generator
    merlin_file.writelines(
        'merlin:\n'
        '   samples: \n'
        '      generate: \n'
        '         cmd: python3 $(SPECROOT)/make_samples.py --filepath=$(MERLIN_INFO)/samples.csv --number=' + str(samples) + '\n'
        '      file: $(MERLIN_INFO)/samples.csv\n'
        '      column_labels: [ID]\n\n'
    )

    # Run job
    merlin_file.writelines(
        'study:\n'
        '    - name: step_1\n'
        '      description: benchmark\n'
        '      run:\n'
        '          cmd: %s\n' % cmd
    )

    merlin_file.close()
    sp.call(['merlin run --local bench.yaml'], shell=True, stdout=sp.PIPE)
    end_time = datetime.now()
    return format(end_time - start_time)


def merlin_benchmark(cmds, data):
    for cmd, samples in cmds:
        time = merlin_cmd(cmd, samples)
        print('Execution of: ' + cmd + '\nTime taken: ' + str(time))
        data['cmd'].append(cmd)
        data['time'].append(time)
    return data


if __name__ == '__main__':
    data = {'cmd': [], 'time': []}

    # Multiple submit bench
    cmds = [["""|
                foo = "$(ID)"
                sleep 0""", 1000]]
    merlin_benchmark(cmds, data)

    # Save to csv
    df = pd.DataFrame(data)
    df.to_csv(csv_loc)

    # Clean
    sp.call(['rm -rf Merlin_Benchmark_*'], shell=True)
