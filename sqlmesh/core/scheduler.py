from __future__ import annotations

import logging
import traceback
import typing as t
from concurrent.futures import Executor, ThreadPoolExecutor, wait
from datetime import datetime
from time import sleep

from sqlmesh.core.console import Console, get_console
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotIdLike
from sqlmesh.core.snapshot_evaluator import SnapshotEvaluator
from sqlmesh.core.state_sync import StateSync
from sqlmesh.utils.date import TimeLike, now, to_datetime, yesterday

logger = logging.getLogger(__name__)
SnapshotBatches = t.List[t.Tuple[Snapshot, t.List[t.Tuple[datetime, datetime]]]]


class Scheduler:
    """Schedules and manages the evaluation of snapshots.

    The scheduler evaluates multiple snapshots with date intervals in the correct
    topological order. It consults the state sync to understand what intervals for each
    snapshot needs to be backfilled.

    The scheduler comes equipped with a simple ThreadPoolExecutor based evaluation engine.

    Args:
        snapshots: A dictionary of all snapshots.
        snapshot_evaluator: The snapshot evaluator to execute queries.
        state_sync: The state sync to pull saved snapshots.
        max_workers: The maximum number of parallel queries to run.
        console: The rich instance used for printing scheduling information.
    """

    def __init__(
        self,
        snapshots: t.Dict[str, Snapshot],
        snapshot_evaluator: SnapshotEvaluator,
        state_sync: StateSync,
        max_workers: int = 1,
        console: t.Optional[Console] = None,
    ):
        self.snapshots = snapshots
        self.snapshot_evaluator = snapshot_evaluator
        self.state_sync = state_sync
        self.max_workers = max_workers
        self.running: t.Set[str] = set()
        self.failed: t.Dict[str, str] = {}
        self.finished: t.Set[str] = set()
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
        try:
            self.snapshot_evaluator.evaluate(
                snapshot,
                start,
                end,
                latest,
                snapshots=self.snapshots,
                **kwargs,
            )
            self.state_sync.add_interval(snapshot.snapshot_id, start, end)
            self.snapshot_evaluator.audit(
                snapshot=snapshot,
                start=start,
                end=end,
                latest=latest,
                snapshots=self.snapshots,
                **kwargs,
            )
            self.console.update_snapshot_progress(snapshot.name, 1)
        except Exception:
            self.failed[snapshot.name] = traceback.format_exc()

    def run(
        self,
        snapshots: t.Iterable[Snapshot],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
    ) -> t.Dict[str, str]:
        """Concurrently runs all snapshots in topological order.

        Args:
            snapshots: An iterable of all the snapshots to run.
            start: The start of the run. Defaults to the min model start date.
            end: The end of the run. Defaults to now.
            latest: The latest datetime to use for non-incremental queries.

        Returns:
            A dictionary of model name to error string.
        """
        snapshots = tuple(snapshots)
        latest = latest or now()
        batches = self.interval_params(snapshots, start, end, latest)

        self.running.clear()
        self.finished.clear()
        self.failed.clear()
        dag = []

        for snapshot, intervals in batches:
            dag.append(
                (
                    snapshot,
                    intervals,
                    {
                        table
                        for table in snapshot.model.depends_on
                        if table in self.snapshots
                    },
                )
            )

        for snapshot, intervals, _ in dag[::-1]:
            if not intervals:
                continue
            # We have to run all batches per snapshot to mark it as completed
            self.console.start_snapshot_progress(snapshot.name, len(intervals))

        with ThreadPoolExecutor() as snapshot_pool, ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as batch_pool:
            while True:
                if self.failed:
                    for model_name, error_message in self.failed.items():
                        self.console.log_error(
                            f"Failed Executing Batch.\nModel name:{model_name}\n{error_message}"
                        )
                    snapshot_pool.shutdown()
                    batch_pool.shutdown()
                    break
                if self.finished >= {snapshot.name for snapshot, _, _ in dag}:
                    break
                processed = self.running | self.finished
                for snapshot, intervals, deps in dag:
                    if snapshot.name not in processed and self.finished >= deps:
                        self.running.add(snapshot.name)
                        snapshot_pool.submit(
                            self._run_snapshot_intervals,
                            snapshot,
                            intervals,
                            latest,
                            batch_pool,
                        )
                sleep(0.1)

        if self.failed:
            self.console.stop_snapshot_progress()
        else:
            self.console.complete_snapshot_progress()

        return self.failed

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

    def _run_snapshot_intervals(
        self,
        snapshot: Snapshot,
        intervals: t.List[t.Tuple[datetime, datetime]],
        latest: TimeLike,
        pool: Executor,
    ) -> None:
        wait(
            [
                pool.submit(self.evaluate, snapshot, start, end, latest)
                for start, end in intervals
            ],
        )
        self.finished.add(snapshot.name)
        self.running.remove(snapshot.name)


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
