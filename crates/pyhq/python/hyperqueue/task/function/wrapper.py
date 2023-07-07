import inspect
import logging

import cloudpickle


class CloudWrapper:
    """
    Wraps a callable so that cloudpickle is used to pickle it, caching the pickle.
    """

    def __init__(self, fn, pickled_fn=None, cache=True, protocol=cloudpickle.DEFAULT_PROTOCOL):
        if fn is None:
            if pickled_fn is None:
                raise ValueError("Pass at least one of `fn` and `pickled_fn`")
            fn = cloudpickle.loads(pickled_fn)
        assert callable(fn)
        # Forget pickled_fn if it should not be cached
        if pickled_fn is not None and not cache:
            pickled_fn = None
        if inspect.isasyncgen(fn):
            raise TypeError("async functions not supported")

        self.fn = fn
        self.pickled_fn = pickled_fn
        self.cache = cache
        self.protocol = protocol
        self.__doc__ = "CloudWrapper for {!r}. Original doc:\n\n{}".format(self.fn, self.fn.__doc__)
        if hasattr(self.fn, "__name__"):
            self.__name__ = self.fn.__name__

        # Build-in functions does not have signature
        try:
            self.__signature__ = inspect.signature(self.fn)
        except ValueError:
            pass

    def is_generator_function(self):
        return inspect.isgeneratorfunction(self.fn)

    def __repr__(self):
        return "<{}({!r})>".format(self.__class__.__name__, self.fn)

    def _get_pickled_fn(self):
        "Get cloudpickled version of self.fn, optionally caching the result"
        if self.pickled_fn is not None:
            return self.pickled_fn

        pfn = cloudpickle.dumps(self.fn, protocol=self.protocol)
        if self.cache:
            self.pickled_fn = pfn
        return pfn

    def __call__(self, *args, **kwargs):
        logging.debug(f"Running function {self.fn} using args {args} and kwargs {kwargs}")
        return self.fn(*args, **kwargs)

    def __reduce__(self):
        return (
            self.__class__,
            (None, self._get_pickled_fn(), self.cache, self.protocol),
        )
