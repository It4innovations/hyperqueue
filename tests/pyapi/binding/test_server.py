from hyperqueue.ffi.ffi import connect_to_server, stop_server

from ...conftest import HqEnv


def test_stop_server(hq_env: HqEnv):
    process = hq_env.start_server()
    ctx = connect_to_server(hq_env.server_dir)
    stop_server(ctx)
    process.wait(timeout=5)
    hq_env.check_process_exited(process)
