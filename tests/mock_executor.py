from concurrent.futures import Future


class MockProcessPoolExecutor:
    """A mock implementation of ProcessPoolExecutor for use in tests.

    This executor runs functions synchronously in the same process, avoiding the issues
    with forking in test environments.
    """

    def __init__(self, max_workers=None, mp_context=None, initializer=None, initargs=()):
        if initializer is not None:
            try:
                initializer(*initargs)
            except BaseException as ex:
                raise RuntimeError(f"Exception in initializer: {ex}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return True

    def submit(self, fn, *args, **kwargs):
        """Execute the function synchronously and return a Future with the result."""
        future = Future()
        try:
            result = fn(*args, **kwargs)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
        return future
