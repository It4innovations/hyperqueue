#!/usr/bin/env python3

import pandas as pd

from hyperqueue import benchmark as hq_bench
from merliner import benchmark as merlin_bench
from snake import benchmark as snake_bench

from glob import glob
from shutil import rmtree


cmds = {'write':
            {
                'snakemake': 'echo {wildcards.sample} > {output}',
                'hyperqueue': 'submit -- bash -c "echo files/\$HQ_JOB_ID"',
                'merlin': """|
                echo 'files/$(ID)'""",
            }
}


def clean_up(tmp_files):
    for path in glob(tmp_files):
        rmtree(path)


if __name__ == '__main__':
    # Create empty data
    data = {'cmd': [], 'time': [], 'system': []}

    samples = 100
    merlin_bench.benchmark(cmds['write']['merlin'], samples, data)
    snake_bench.benchmark(cmds['write']['snakemake'], samples, data)
    hq_bench.benchmark(cmds['write']['hyperqueue'], samples, data)

    # Save to csv
    df = pd.DataFrame(data)
    df.to_csv('results.csv')

    # Clean
    clean_up('Merlin_Benchmark_*')
    clean_up('job-*')
    clean_up('files')
