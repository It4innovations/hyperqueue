import subprocess as sp

from datetime import datetime


def merlin_cmd(cmd, samples):
    start_time = datetime.now()

    with open("bench.yaml", "w") as merlin_file:
        merlin_file.write(
            'description:\n'
            '   name: Merlin Benchmark\n'
            '   description: Simple bench\n\n'
        )

        # Create merlin generator
        merlin_file.writelines(
            'merlin:\n'
            '   samples: \n'
            '      generate: \n'
            '         cmd: python3 $(SPECROOT)/merliner/make_samples.py --filepath=$(MERLIN_INFO)/samples.csv --number=' + str(samples) + '\n'
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

    sp.call(['merlin run --local bench.yaml'], shell=True, stdout=sp.PIPE)
    end_time = datetime.now()
    return (end_time - start_time).total_seconds()


def benchmark(cmd, samples, data):
    time = merlin_cmd(cmd, samples)
    print(f"Execution of {cmd}\nTime taken: {time}")
    data['cmd'].append(cmd)
    data['time'].append(time)
    data['system'].append('merlin')

    return data
