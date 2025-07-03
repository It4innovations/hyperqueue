# Iterative computation
It is a common use-case to perform an iterative computation, e.g. run a randomized simulation until the results are
stable/accurate enough, or train a machine learning model while the loss keeps dropping.

While there is currently no built-in support in HQ for iteratively submitting new tasks to an existing job, you can perform
an iterative computation relatively easily with the following approach:

1. Submit a HQ job that performs a computation
2. Wait for the job to finish
3. Read the output of the job and decide if computation should continue
4. If yes, go to 1.

## Command-line interface
With the command-line interface, you can perform the iterative loop e.g. in Bash.

```bash
#!/bin/bash

while :
do
  # Submit a job and wait for it to complete
  ./hq submit --wait ./compute.sh
  
  # Read the output of the job
  output=$(./hq job cat last stdout)

  # Decide if we should end or continue
  if [ "${output}" -eq 0 ]; then
      break
  fi
done
```

## Python API
With the Python API, we can simply write the outermost iteration loop in Python, and repeatedly submit jobs, until some
end criterion has been achieved:

```python
from hyperqueue import Job, Client

client = Client()

while True:
    job = Job()
    job.program(["my-program"], stdout="out.txt")

    # Submit a job
    submitted = client.submit(job)

    # Wait for it to complete
    client.wait_for_jobs([submitted])

    # Read the output of the job
    with open("out.txt") as f:
        # Check some termination condition and eventually end the loop
        if f.read().strip() == "done":
            break
```
