# Data arrays
Executing the same program for each item of an array of data is a perfect use-case for HyperQueue. It contains built-in support for generating a [task array](../../jobs/arrays.md) from a file containing a [JSON array](../../jobs/arrays.md#json-array) or from a file where each task input is specified on a [separate line](../../jobs/arrays.md#lines-of-a-file).

## Processing many input files with the same program
Let's say that we have a directory with 100 data files that we want to process using some program.

First, we create an input file (called e.g. `inputs.txt`) that will store the filepaths of all these data files:

```text
/data/input-01.txt
/data/input-02.txt
...
```

Then we create a bash script called e.g. `compute.sh` that will be executed by each HyperQueue task. Each such task will receive a single line from `inputs.txt` in the `HQ_ENTRY` environment variable. Our bash script will simply forward this line to a program of our choosing:

```bash
#!/bin/bash

/home/user/my-program --param a=b --input ${HQ_ENTRY}
```

And finally, we can submit a task graph where a single task will be spawned for each line in the file above using the following command:

```bash
$ hq submit --each-line=inputs.txt ./compute.sh
```

If the `inputs.txt` file contained 100 lines, the command above would create a single job with 100 tasks.
