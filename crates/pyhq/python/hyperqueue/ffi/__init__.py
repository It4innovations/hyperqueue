from .. import hyperqueue as ffi  # noqa

JobId = int
TaskId = int


def get_version() -> str:
    return ffi.get_hq_version()
