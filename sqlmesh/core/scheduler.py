from __future__ import annotations

import logging
import traceback
import typing as t
from datetime import datetime

from sqlmesh.core.console import Console, get_console
from sqlmesh.core.dag import DAG
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotIdLike
from sqlmesh.core.snapshot_evaluator import SnapshotEvaluator
from sqlmesh.core.state_sync import StateSync
from sqlmesh.utils.concurrency import NodeExecutionFailedError, concurrent_apply_to_dag
from sqlmesh.utils.date import (
    TimeLike,
    now,
    to_datetime,
    validate_date_range,
    yesterday,
)

logger = logging.getLogger(__name__)
SnapshotBatches = t.List[t.Tuple[Snapshot, t.List[t.Tuple[datetime, datetime]]]]
SchedulingUnit = t.Tuple[SnapshotId, t.Tuple[datetime, datetime]]


class Scheduler:
    """Schedules and manages the evaluation of snapshots.

    The scheduler evaluates multiple snapshots with date intervals in the correct
    topological order. It consults the state sync to understand what intervals for each
    snapshot needs to be backfilled.

    The scheduler comes equipped with a simple ThreadPoolExecutor based evaluation engine.

    Args:
        snapshots: A collection of snapshots.
        snapshot_evaluator: The snapshot evaluator to execute queries.
        state_sync: The state sync to pull saved snapshots.
        max_workers: The maximum number of parallel queries to run.
        console: The rich instance used for printing scheduling information.
    """

    def __init__(
        self,
        snapshots: t.Iterable[Snapshot],
        snapshot_evaluator: SnapshotEvaluator,
        state_sync: StateSync,
        max_workers: int = 1,
        console: t.Optional[Console] = None,
    ):
        self.snapshots = {s.snapshot_id: s for s in snapshots}
        self.snapshot_evaluator = snapshot_evaluator
        self.state_sync = state_sync
        self.max_workers = max_workers
        self.console: Console = console or get_console()

    def evaluate(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        latest: TimeLike,
        **kwargs,
    ) -> None:
        """Evaluate a snapshot and add the processed interval to the state sync.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            latest: The latest datetime to use for non-incremental queries.
            kwargs: Additional kwargs to pass to the renderer.
        """
        validate_date_range(start, end)

        mapping = {
            **{
                p_sid.name: self.snapshots[p_sid].table_name
                for p_sid in snapshot.parents
            },
            snapshot.name: snapshot.table_name,
        }

        self.snapshot_evaluator.evaluate(
            snapshot,
            start,
            end,
            latest,
            mapping=mapping,
            **kwargs,
        )
        self.state_sync.add_interval(snapshot.snapshot_id, start, end)
        self.snapshot_evaluator.audit(
            snapshot=snapshot,
            start=start,
            end=end,
            latest=latest,
            mapping=mapping,
            **kwargs,
        )
        self.console.update_snapshot_progress(snapshot.name, 1)

    def run(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
    ) -> None:
        """Concurrently runs all snapshots in topological order.

        Args:
            start: The start of the run. Defaults to the min model start date.
            end: The end of the run. Defaults to now.
            latest: The latest datetime to use for non-incremental queries.
        """
        validate_date_range(start, end)

        latest = latest or now()
        batches = self.interval_params(self.snapshots.values(), start, end, latest)

        intervals_per_snapshot_id = {
            snapshot.snapshot_id: intervals for snapshot, intervals in batches
        }

        dag = DAG[SchedulingUnit]()
        for snapshot, intervals in batches:
            upstream_dependencies = [
                (p_sid, interval)
                for p_sid in snapshot.parents
                for interval in intervals_per_snapshot_id.get(p_sid, [])
            ]
            sid = snapshot.snapshot_id
            for interval in intervals:
                dag.add((sid, interval), upstream_dependencies)
            self.console.start_snapshot_progress(snapshot.name, len(intervals))

        def evaluate_node(node: SchedulingUnit) -> None:
            assert latest
            sid, (start, end) = node
            self.evaluate(self.snapshots[sid], start, end, latest)

        try:
            with self.snapshot_evaluator.concurrent_context():
                concurrent_apply_to_dag(dag, evaluate_node, self.max_workers)
        except NodeExecutionFailedError as error:
            sid = error.node[0]  # type: ignore
            self.console.log_error(
                f"Failed Executing Batch.\nSnapshot: {sid}\n{traceback.format_exc()}"
            )
            self.console.stop_snapshot_progress()
            raise

        self.console.complete_snapshot_progress()

    def interval_params(
        self,
        snapshots: t.Iterable[Snapshot],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
    ) -> SnapshotBatches:
        """Find the optimal date interval paramaters based on what needs processing and maximal batch size.

        For each model name, find all dependencies and look for a stored snapshot from the metastore. If a snapshot is found,
        calculate the missing intervals that need to be processed given the passed in start and end intervals.

        If a snapshot's model specifies a batch size, consecutive intervals are merged into batches of a size that is less than
        or equal to the configured one. If no batch size is specified, then it uses the intervals that correspond to the model's cron expression.
        For example, if a model is supposed to run daily and has 70 days to backfill with a batch size set to 30, there would be 2 jobs
        with 30 days and 1 job with 10.

        Args:
            model_names: The list of model names.
            start: Start of the interval.
            end: End of the interval.
            latest: The latest datetime to use for non-incremental queries.

        Returns:
            A list of tuples containing all snapshots needing to be run with their associated interval params.
        """
        stored_snapshots = self.state_sync.get_snapshots_with_same_version(snapshots)
        all_snapshots = {
            s.snapshot_id: s for s in (list(self.snapshots.values()) + stored_snapshots)
        }
        return compute_interval_params(
            snapshots,
            snapshots=all_snapshots,
            start=start or earliest_start_date(snapshots),
            end=end or now(),
            latest=latest or now(),
        )


def compute_interval_params(
    target: t.Iterable[SnapshotIdLike],
    *,
    snapshots: t.Dict[SnapshotId, Snapshot],
    start: TimeLike,
    end: TimeLike,
    latest: TimeLike,
) -> SnapshotBatches:
    """Find the optimal date interval paramaters based on what needs processing and maximal batch size.

    For each model name, find all dependencies and look for a stored snapshot from the metastore. If a snapshot is found,
    calculate the missing intervals that need to be processed given the passed in start and end intervals.

    If a snapshot's model specifies a batch size, consecutive intervals are merged into batches of a size that is less than
    or equal to the configured one. If no batch size is specified, then it uses the intervals that correspond to the model's cron expression.
    For example, if a model is supposed to run daily and has 70 days to backfill with a batch size set to 30, there would be 2 jobs
    with 30 days and 1 job with 10.

    Args:
        target: A set of target snapshots for which intervals should be computed.
        snapshots: A catalog of all available snapshots (including the target ones).
        start: Start of the interval.
        end: End of the interval.
        latest: The latest datetime to use for non-incremental queries.

    Returns:
        A list of tuples containing all snapshots needing to be run with their associated interval params.
    """
    start_dt = to_datetime(start)

    params = []

    for snapshot in Snapshot.merge_snapshots(target, snapshots):
        model_start_dt = max(
            start_date(snapshot, snapshots.values()) or start_dt, start_dt
        )
        params.append(
            (
                snapshot,
                [
                    (to_datetime(s), to_datetime(e))
                    for s, e in snapshot.missing_intervals(model_start_dt, end, latest)
                ],
            )
        )

    return _batched_intervals(params)


def start_date(
    snapshot: Snapshot, snapshots: t.Dict[str, Snapshot] | t.Iterable[Snapshot]
) -> t.Optional[datetime]:
    """Get the effective/inferred start date for a snapshot.

    Not all snapshots define a start date. In those cases, the model's start date
    can be inferred from its parent's start date.

    Args:
        snapshot: snapshot to infer start date.
        snapshots: a catalog of available snapshots.

    Returns:
        Start datetime object.
    """
    if snapshot.model.start:
        return to_datetime(snapshot.model.start)

    if not isinstance(snapshots, dict):
        snapshots = {snapshot.name: snapshot for snapshot in snapshots}

    earliest = None

    for parent in snapshot.parents:
        start_dt = start_date(snapshots[parent.name], snapshots)

        if not earliest:
            earliest = start_dt
        elif start_dt:
            earliest = min(earliest, start_dt)

    return earliest


def earliest_start_date(snapshots: t.Iterable[Snapshot]) -> datetime:
    """Get the earliest start date from a collection of snapshots.

    Args:
        snapshots: Snapshots to find earliest start date.
    Returns:
        The earliest start date or yesterday if none is found."""
    snapshots = list(snapshots)
    return min(start_date(snapshot, snapshots) or yesterday() for snapshot in snapshots)


def _batched_intervals(params: SnapshotBatches) -> SnapshotBatches:
    batches = []

    for snapshot, intervals in params:
        model = snapshot.model
        if model.batch_size:
            batches_for_snapshot = []
            next_batch: t.List[t.Tuple[datetime, datetime]] = []
            for interval in intervals:
                if len(next_batch) >= model.batch_size or (
                    next_batch and interval[0] != next_batch[-1][-1]
                ):
                    batches_for_snapshot.append((next_batch[0][0], next_batch[-1][-1]))
                    next_batch = []
                next_batch.append(interval)
            if next_batch:
                batches_for_snapshot.append((next_batch[0][0], next_batch[-1][-1]))
            batches.append((snapshot, batches_for_snapshot))
        else:
            batches.append((snapshot, intervals))

    return batches
