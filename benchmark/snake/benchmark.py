#!/usr/bin/env python3

import pandas as pd
import subprocess as sp
import time

from datetime import datetime


csv_loc = "snake_bench.csv"


def snake_cmd(cmd, samples):
    start_time = datetime.now()
    snake_file = open("Snakefile", "w")

    # Create potential files
    snake_file.writelines(
        'rule all:\n'
        '\tinput: \n'
        '\t\texpand("files/{sample}", sample=range(%d))\n' % samples
    )

    # Exectute some command on these jobs
    snake_file.writelines(
        'rule benchmark:\n'
        '\toutput: \n'
        '\t\t"{sample}"\n'
        '\tshell: \n'
        '\t\t"%s"' % cmd
    )
    snake_file.close()
    sp.call(['snakemake --cores all'], shell=True, stdout=sp.PIPE)
    end_time = datetime.now()
    return format(end_time - start_time)


def snake_benchmark(cmds, data):
    for cmd, samples in cmds:
        time = snake_cmd(cmd, samples)
        print('Execution of: ' + cmd + '\nTime taken: ' + str(time))
        data['cmd'].append(cmd)
        data['time'].append(time)
    return data


if __name__ == '__main__':
    data = {'cmd': [], 'time': []}

    # Multiple submit bench
    cmds = [["sleep 0 > {output}", 1000]]
    snake_benchmark(cmds, data)

    # Save to csv
    df = pd.DataFrame(data)
    df.to_csv(csv_loc)

    # Clean
    sp.call(['rm -rf files'], shell=True)

