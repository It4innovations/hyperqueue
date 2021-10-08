import subprocess as sp

from datetime import datetime


def snake_cmd(cmd, samples):
    start_time = datetime.now()

    with open("Snakefile", "w") as snake_file:
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

    sp.call(['snakemake --quiet --cores 1'], shell=True, stdout=sp.PIPE)
    end_time = datetime.now()
    return (end_time - start_time).total_seconds()


def benchmark(cmd, samples, data):
    time = snake_cmd(cmd, samples)
    print(f"Execution of {cmd}\nTime taken: {time}")
    data['cmd'].append(cmd)
    data['time'].append(time)
    data['system'].append('snakemake')

    return data
