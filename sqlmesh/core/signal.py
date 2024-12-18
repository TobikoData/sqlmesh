from __future__ import annotations


from sqlmesh.utils import UniqueKeyDict, registry_decorator


class signal(registry_decorator):
    """Specifies a function which intervals are ready from a list of scheduled intervals.

    When SQLMesh wishes to execute a batch of intervals, say between `a` and `d`, then
    the `batch` parameter will contain each individual interval within this batch,
    i.e.: `[a,b),[b,c),[c,d)`.

    This function may return `True` to indicate that the whole batch is ready,
    `False` to indicate none of the batch's intervals are ready, or a list of
    intervals (a batch) to indicate exactly which ones are ready.

    When returning a batch, the function is expected to return a subset of
    the `batch` parameter, e.g.: `[a,b),[b,c)`. Note that it may return
    gaps, e.g.: `[a,b),[c,d)`, but it may not alter the bounds of any of the
    intervals.

    The interface allows an implementation to check batches of intervals without
    having to actually compute individual intervals itself.

    Args:
        batch: the list of intervals that are missing and scheduled to run.

    Returns:
        Either `True` to indicate all intervals are ready, `False` to indicate none are
        ready or a list of intervals to indicate exactly which ones are ready.
    """


SignalRegistry = UniqueKeyDict[str, signal]
