class Init:
    pass


class Started:
    pass


class Stopped:
    pass


class EnvStateManager:
    """
    Helper mixin class that makes sure that an environment is used in the correct order and that
    it is not started/stopped multiple times.
    """

    def __init__(self):
        self.state = Init()

    def state_start(self):
        assert isinstance(self.state, Init)
        self.state = Started()

    def state_stop(self):
        assert isinstance(self.state, Started)
        self.state = Stopped()
