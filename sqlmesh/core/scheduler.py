from __future__ import annotations

import logging
import typing as t
from datetime import datetime

from sqlmesh.core.console import Console, get_console
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotEvaluator,
    SnapshotId,
    SnapshotIdLike,
)
from sqlmesh.core.state_sync import StateSync
from sqlmesh.utils import format_exception
from sqlmesh.utils.concurrency import concurrent_apply_to_dag
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import (
    TimeLike,
    now,
    to_datetime,
    validate_date_range,
    yesterday,
)

logger = logging.getLogger(__name__)
Interval = t.Tuple[datetime, datetime]
Batch = t.List[Interval]
SnapshotToBatches = t.Dict[Snapshot, Batch]
SchedulingUnit = t.Tuple[Snapshot, Interval]


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
        self.snapshot_per_version = _resolve_one_snapshot_per_version(snapshots)
        self.snapshot_evaluator = snapshot_evaluator
        self.state_sync = state_sync
        self.max_workers = max_workers
        self.console: Console = console or get_console()

    def batches(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        is_dev: bool = False,
    ) -> SnapshotToBatches:
        """Returns a list of snapshot batches to evaluate.

        Args:
            start: The start of the run. Defaults to the min model start date.
            end: The end of the run. Defaults to now.
            latest: The latest datetime to use for non-incremental queries.
            is_dev: Indicates whether the evaluation happens in the development mode and temporary
                tables / table clones should be used where applicable.
        """
        validate_date_range(start, end)

        return self._interval_params(
            self.snapshot_per_version.values(),
            start,
            end,
            latest,
            is_dev=is_dev,
        )

    def evaluate(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        latest: TimeLike,
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """Evaluate a snapshot and add the processed interval to the state sync.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            latest: The latest datetime to use for non-incremental queries.
            is_dev: Indicates whether the evaluation happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.
        """
        validate_date_range(start, end)

        snapshots = {
            **{p_sid.name: self.snapshots[p_sid] for p_sid in snapshot.parents},
            snapshot.name: snapshot,
        }

        self.snapshot_evaluator.evaluate(
            snapshot,
            start,
            end,
            latest,
            snapshots=snapshots,
            is_dev=is_dev,
            **kwargs,
        )
        self.snapshot_evaluator.audit(
            snapshot=snapshot,
            start=start,
            end=end,
            latest=latest,
            snapshots=snapshots,
            is_dev=is_dev,
            **kwargs,
        )
        self.state_sync.add_interval(snapshot.snapshot_id, start, end, is_dev=is_dev)
        self.console.update_snapshot_progress(snapshot.name, 1)

    def run(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        is_dev: bool = False,
    ) -> bool:
        """Concurrently runs all snapshots in topological order.

        Args:
            start: The start of the run. Defaults to the min model start date.
            end: The end of the run. Defaults to now.
            latest: The latest datetime to use for non-incremental queries.
            is_dev: Indicates whether the evaluation happens in the development mode and temporary
                tables / table clones should be used where applicable.

        Returns:
            True if the execution was successful and False otherwise.
        """
        validate_date_range(start, end)

        latest = latest or now()
        batches = self.batches(start, end, latest, is_dev=is_dev)
        dag = self._dag(batches)

        visited = set()
        for snapshot, _ in dag.sorted():
            if snapshot in visited:
                continue
            visited.add(snapshot)
            intervals = batches[snapshot]
            self.console.start_snapshot_progress(snapshot.name, len(intervals))

        def evaluate_node(node: SchedulingUnit) -> None:
            assert latest
            snapshot, (start, end) = node
            self.evaluate(snapshot, start, end, latest, is_dev=is_dev)

        with self.snapshot_evaluator.concurrent_context():
            errors, skipped_intervals = concurrent_apply_to_dag(
                dag,
                evaluate_node,
                self.max_workers,
                raise_on_error=False,
            )

        self.console.stop_snapshot_progress(success=not errors)

        for error in errors:
            sid = error.node[0]
            formatted_exception = "".join(format_exception(error.__cause__ or error))
            self.console.log_error(f"FAILED processing snapshot {sid}\n{formatted_exception}")

        skipped_snapshots = {i[0] for i in skipped_intervals}
        for skipped in skipped_snapshots:
            self.console.log_status_update(f"SKIPPED snapshot {skipped}\n")

        return not errors

    def _interval_params(
        self,
        snapshots: t.Iterable[Snapshot],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        is_dev: bool = False,
    ) -> SnapshotToBatches:
        """Find the optimal date interval paramaters based on what needs processing and maximal batch size.

        For each model name, find all dependencies and look for a stored snapshot from the metastore. If a snapshot is found,
        calculate the missing intervals that need to be processed given the passed in start and end intervals.

        If a snapshot's model specifies a batch size, consecutive intervals are merged into batches of a size that is less than
        or equal to the configured one. If no batch size is specified, then it uses the intervals that correspond to the model's cron expression.
        For example, if a model is supposed to run daily and has 70 days to backfill with a batch size set to 30, there would be 2 jobs
        with 30 days and 1 job with 10.

        Args:
            snapshots: The list of snapshots.
            start: Start of the interval.
            end: End of the interval.
            latest: The latest datetime to use for non-incremental queries.
            is_dev: Indicates whether the evaluation happens in the development mode.

        Returns:
            A list of tuples containing all snapshots needing to be run with their associated interval params.
        """
        all_snapshots = {s.snapshot_id: s for s in self.snapshots.values()}

        # When in development mode only consider intervals of the current forward-only snapshot and ignore
        # intervals of all snapshots with the same version that came before it.
        same_version_snapshots = (
            [s for s in snapshots if not s.is_forward_only or not s.is_paused]
            if is_dev
            else snapshots
        )
        stored_snapshots = self.state_sync.get_snapshots_with_same_version(same_version_snapshots)
        all_snapshots.update({s.snapshot_id: s for s in stored_snapshots})

        return compute_interval_params(
            snapshots,
            snapshots=all_snapshots,
            start=start or earliest_start_date(snapshots),
            end=end or now(),
            latest=latest or now(),
        )

    def _dag(self, batches: SnapshotToBatches) -> DAG[SchedulingUnit]:
        """Builds a DAG of snapshot intervals to be evaluated.

        Args:
            batches: The batches of snapshots and intervals to evaluate.

        Returns:
            A DAG of snapshot intervals to be evaluated.
        """

        intervals_per_snapshot_version = {
            (snapshot.name, snapshot.version_get_or_generate()): intervals
            for snapshot, intervals in batches.items()
        }

        dag = DAG[SchedulingUnit]()
        for snapshot, intervals in batches.items():
            if not intervals:
                continue
            upstream_dependencies = [
                (self.snapshots[p_sid], interval)
                for p_sid in snapshot.parents
                if p_sid in self.snapshots
                for interval in intervals_per_snapshot_version.get(
                    (
                        self.snapshots[p_sid].name,
                        self.snapshots[p_sid].version_get_or_generate(),
                    ),
                    [],
                )
            ]
            for i, interval in enumerate(intervals):
                dag.add((snapshot, interval), upstream_dependencies)
                if snapshot.is_incremental_by_unique_key_kind:
                    dag.add(
                        (snapshot, interval),
                        [(snapshot, _interval) for _interval in intervals[:i]],
                    )

        return dag


def compute_interval_params(
    target: t.Iterable[SnapshotIdLike],
    *,
    snapshots: t.Dict[SnapshotId, Snapshot],
    start: TimeLike,
    end: TimeLike,
    latest: TimeLike,
) -> SnapshotToBatches:
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
        A dict containing all snapshots needing to be run with their associated interval params.
    """
    start_dt = to_datetime(start)

    snapshots_to_batches = {}

    for snapshot in Snapshot.merge_snapshots(target, snapshots):
        model_start_dt = max(start_date(snapshot, snapshots.values()) or start_dt, start_dt)
        snapshots_to_batches[snapshot] = [
            (to_datetime(s), to_datetime(e))
            for s, e in snapshot.missing_intervals(model_start_dt, end, latest)
        ]

    return _batched_intervals(snapshots_to_batches)


def start_date(
    snapshot: Snapshot, snapshots: t.Dict[SnapshotId, Snapshot] | t.Iterable[Snapshot]
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
        snapshots = {snapshot.snapshot_id: snapshot for snapshot in snapshots}

    earliest = None

    for parent in snapshot.parents:
        if parent not in snapshots:
            continue

        start_dt = start_date(snapshots[parent], snapshots)

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
        The earliest start date or yesterday if none is found.
    """
    snapshots = list(snapshots)
    if snapshots:
        return min(start_date(snapshot, snapshots) or yesterday() for snapshot in snapshots)
    return yesterday()


def _batched_intervals(params: SnapshotToBatches) -> SnapshotToBatches:
    batches = {}

    for snapshot, intervals in params.items():
        batch_size = snapshot.model.batch_size
        batches_for_snapshot = []
        next_batch: t.List[Interval] = []
        for interval in intervals:
            if (batch_size and len(next_batch) >= batch_size) or (
                next_batch and interval[0] != next_batch[-1][-1]
            ):
                batches_for_snapshot.append((next_batch[0][0], next_batch[-1][-1]))
                next_batch = []
            next_batch.append(interval)
        if next_batch:
            batches_for_snapshot.append((next_batch[0][0], next_batch[-1][-1]))
        batches[snapshot] = batches_for_snapshot

    return batches


def _resolve_one_snapshot_per_version(
    snapshots: t.Iterable[Snapshot],
) -> t.Dict[t.Tuple[str, str], Snapshot]:
    snapshot_per_version: t.Dict[t.Tuple[str, str], Snapshot] = {}
    for snapshot in snapshots:
        key = (snapshot.name, snapshot.version_get_or_generate())
        if key not in snapshot_per_version:
            snapshot_per_version[key] = snapshot
        else:
            prev_snapshot = snapshot_per_version[key]
            if snapshot.unpaused_ts and (
                not prev_snapshot.unpaused_ts or snapshot.created_ts > prev_snapshot.created_ts
            ):
                snapshot_per_version[key] = snapshot

    return snapshot_per_version
