# HyperQueue test suite
This directory contains the Python test suite of HyperQueue.

## Usage
The following commands are supposed to be executed from the root HyperQueue directory.

1) Install `pytest` and other dependencies
    ```bash
    $ python -m pip install tests/requirements.txt
    ```
2) Run tests
    ```bash
    $ python -m pytest tests
    ```

You can speed up test execution by running them in parallel:
```bash
$ python -m pytest tests -n16
```

### Running autoalloc tests
There are several tests for the automatic allocator that require the presence of an external service (PBS).
If you are on a system that has these services installed, you can run these tests with the `pbs` mark:
```bash
$ python -m pytest tests -m pbs
```

## Blessing
You can bless tests with the following command:
```bash
$ python -m pytest --inline-snapshot=create
```
