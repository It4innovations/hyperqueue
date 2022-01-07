from . import pyhq as ffi


def connect_to_server():
    return ffi.connect_to_server()


def stop_server(ctx):
    return ffi.stop_server(ctx)
