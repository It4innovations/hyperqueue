from hyperqueue.ffi.connection import Connection

from ...conftest import HqEnv


def test_stop_server(hq_env: HqEnv):
    process = hq_env.start_server()
    connection = Connection(hq_env.server_dir)
    connection.stop_server()
    process.wait(timeout=5)
    hq_env.check_process_exited(process)
