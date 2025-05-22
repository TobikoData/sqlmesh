# mypy: disable-error-code=no-untyped-def

from concurrent.futures import Future, ProcessPoolExecutor
import typing as t
import multiprocessing as mp
from sqlmesh.utils.windows import IS_WINDOWS


class SynchronousPoolExecutor:
    """A mock implementation of the ProcessPoolExecutor for synchronous use.

    This executor runs functions synchronously in the same process, avoiding the issues
    with forking in test environments or when forking isn't possible (non-posix).
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
        self.shutdown(wait=True)
        return False

    def shutdown(self, wait=True, cancel_futures=False):
        """No-op method to match ProcessPoolExecutor API.

        Since this executor runs synchronously, there are no background processes
        or resources to shut down and all futures will have completed already.
        """
        pass

    def submit(self, fn, *args, **kwargs):
        """Execute the function synchronously and return a Future with the result."""
        future = Future()
        try:
            result = fn(*args, **kwargs)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
        return future

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        """Synchronous implementation of ProcessPoolExecutor.map.

        This executes the function for each set of inputs from the iterables in the
        current process using Python's built-in map, rather than distributing work.
        """
        return map(fn, *iterables)


PoolExecutor = t.Union[SynchronousPoolExecutor, ProcessPoolExecutor]


def create_process_pool_executor(
    initializer: t.Callable, initargs: t.Tuple, max_workers: t.Optional[int]
) -> PoolExecutor:
    if max_workers == 1 or IS_WINDOWS:
        return SynchronousPoolExecutor(
            initializer=initializer,
            initargs=initargs,
        )
    return ProcessPoolExecutor(
        mp_context=mp.get_context("fork"),
        initializer=initializer,
        initargs=initargs,
        max_workers=max_workers,
    )
