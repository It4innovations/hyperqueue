# PyHQ binding
This package provides a Python binding to HyperQueue.

## Development
1) Install `maturin`
```bash
$ pip install maturin
```
2) Build the bindings
```bash
$ maturin develop
```
3) Use the build `pyhq` package
```python
from pyhq.client import connect_to_server, stop_server

ctx = connect_to_server()
stop_server(ctx)
```
