from __future__ import annotations

import typing as t
from sqlmesh.utils import UniqueKeyDict, registry_decorator

if t.TYPE_CHECKING:
    from sqlmesh.core.context import ExecutionContext
    from sqlmesh.core.snapshot.definition import Snapshot
    from sqlmesh.utils.date import DatetimeRanges
    from sqlmesh.core.snapshot.definition import DeployabilityIndex


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


@signal()
def freshness(batch: DatetimeRanges, snapshot: Snapshot, context: ExecutionContext) -> bool:
    adapter = context.engine_adapter
    if context.is_restatement or not adapter.SUPPORTS_METADATA_TABLE_LAST_MODIFIED_TS:
        return True

    deployability_index = context.deployability_index or DeployabilityIndex.all_deployable()

    last_altered_ts = (
        snapshot.last_altered_ts
        if deployability_index.is_deployable(snapshot)
        else snapshot.dev_last_altered_ts
    )
    if not last_altered_ts:
        return True

    parent_snapshots = {context.snapshots[p.name] for p in snapshot.parents}
    if len(parent_snapshots) != len(snapshot.node.depends_on) or not all(
        p.is_external for p in parent_snapshots
    ):
        # The mismatch can happen if e.g an external model is not registered in the project
        return True

    # Finding new data means that the upstream depedencies have been altered
    # since the last time the model was evaluated
    upstream_dep_has_new_data = any(
        upstream_last_altered_ts > last_altered_ts
        for upstream_last_altered_ts in adapter.get_table_last_modified_ts(
            [p.name for p in parent_snapshots]
        )
    )

    # Returning true is a no-op, returning False nullifies the batch so the model will not be evaluated.
    return upstream_dep_has_new_data
