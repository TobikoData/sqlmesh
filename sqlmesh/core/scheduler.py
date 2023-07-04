from __future__ import annotations

import logging
import typing as t
from datetime import datetime

from sqlmesh.core import constants as c
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.model import SeedModel
from sqlmesh.core.notification_target import (
    NotificationEvent,
    NotificationTargetManager,
)
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotEvaluator,
    SnapshotId,
    SnapshotIntervals,
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
from sqlmesh.utils.errors import AuditError

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
        notification_target_manager: t.Optional[NotificationTargetManager] = None,
    ):
        self.snapshots = {s.snapshot_id: s for s in snapshots}
        self.snapshot_per_version = _resolve_one_snapshot_per_version(snapshots)
        self.snapshot_evaluator = snapshot_evaluator
        self.state_sync = state_sync
        self.max_workers = max_workers
        self.console: Console = console or get_console()
        self.notification_target_manager = (
            notification_target_manager or NotificationTargetManager()
        )

    def batches(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        is_dev: bool = False,
        restatements: t.Optional[t.Set[str]] = None,
    ) -> SnapshotToBatches:
        """Returns a list of snapshot batches to evaluate.

        Args:
            start: The start of the run. Defaults to the min model start date.
            end: The end of the run. Defaults to now.
            latest: The latest datetime to use for non-incremental queries.
            is_dev: Indicates whether the evaluation happens in the development mode and temporary
                tables / table clones should be used where applicable.
            restatements: A set of snapshot names being restated.
        """
        restatements = restatements or set()
        validate_date_range(start, end)

        return self._interval_params(
            self.snapshot_per_version.values(),
            start,
            end,
            latest,
            is_dev=is_dev,
            restatements=restatements,
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

        if isinstance(snapshot.model, SeedModel) and not snapshot.model.is_hydrated:
            snapshot = self.state_sync.get_snapshots([snapshot], hydrate_seeds=True)[
                snapshot.snapshot_id
            ]

        self.snapshot_evaluator.evaluate(
            snapshot,
            start,
            end,
            latest,
            snapshots=snapshots,
            is_dev=is_dev,
            **kwargs,
        )
        try:
            self.snapshot_evaluator.audit(
                snapshot=snapshot,
                start=start,
                end=end,
                latest=latest,
                snapshots=snapshots,
                is_dev=is_dev,
                **kwargs,
            )
        except AuditError as e:
            self.notification_target_manager.notify(NotificationEvent.AUDIT_FAILURE, e)
            if not is_dev and snapshot.model.owner:
                self.notification_target_manager.notify_user(
                    NotificationEvent.AUDIT_FAILURE, snapshot.model.owner, e
                )
            raise e
        self.state_sync.add_interval(snapshot, start, end, is_dev=is_dev)
        self.console.update_snapshot_progress(snapshot.name, 1)

    def run(
        self,
        environment: str,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        restatements: t.Optional[t.Set[str]] = None,
    ) -> bool:
        """Concurrently runs all snapshots in topological order.

        Args:
            environment: The environment the user is targeting when applying their change.
            start: The start of the run. Defaults to the min model start date.
            end: The end of the run. Defaults to now.
            latest: The latest datetime to use for non-incremental queries.
            restatements: A set of snapshots to restate.

        Returns:
            True if the execution was successful and False otherwise.
        """
        restatements = restatements or set()
        validate_date_range(start, end)

        is_dev = environment != c.PROD
        latest = latest or now()
        batches = self.batches(start, end, latest, is_dev=is_dev, restatements=restatements)
        dag = self._dag(batches)

        visited = set()
        for snapshot, _ in dag.sorted:
            if snapshot in visited:
                continue
            visited.add(snapshot)
            intervals = batches[snapshot]
            self.console.start_snapshot_progress(snapshot, len(intervals), environment)

        def evaluate_node(node: SchedulingUnit) -> None:
            assert latest
            snapshot, (start, end) = node
            self.evaluate(snapshot, start, end, latest, is_dev=is_dev)

        try:
            with self.snapshot_evaluator.concurrent_context():
                errors, skipped_intervals = concurrent_apply_to_dag(
                    dag,
                    evaluate_node,
                    self.max_workers,
                    raise_on_error=False,
                )
        finally:
            self.state_sync.recycle()

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
        restatements: t.Optional[t.Set[str]] = None,
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
            restatements: A set of snapshots to restate.

        Returns:
            A list of tuples containing all snapshots needing to be run with their associated interval params.
        """
        return compute_interval_params(
            snapshots,
            intervals=self.state_sync.get_snapshot_intervals(snapshots),
            start=start or earliest_start_date(snapshots),
            end=end or now(),
            latest=latest or now(),
            is_dev=is_dev,
            restatements=restatements,
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
                if snapshot.depends_on_past:
                    dag.add(
                        (snapshot, interval),
                        [(snapshot, _interval) for _interval in intervals[:i]],
                    )
        return dag


def compute_interval_params(
    snapshots: t.Iterable[Snapshot],
    *,
    intervals: t.Iterable[SnapshotIntervals],
    start: TimeLike,
    end: TimeLike,
    latest: TimeLike,
    is_dev: bool,
    restatements: t.Optional[t.Set[str]] = None,
) -> SnapshotToBatches:
    """Find the optimal date interval paramaters based on what needs processing and maximal batch size.

    For each model name, find all dependencies and look for a stored snapshot from the metastore. If a snapshot is found,
    calculate the missing intervals that need to be processed given the passed in start and end intervals.

    If a snapshot's model specifies a batch size, consecutive intervals are merged into batches of a size that is less than
    or equal to the configured one. If no batch size is specified, then it uses the intervals that correspond to the model's cron expression.
    For example, if a model is supposed to run daily and has 70 days to backfill with a batch size set to 30, there would be 2 jobs
    with 30 days and 1 job with 10.

    Args:
        snapshots: A set of target snapshots for which intervals should be computed.
        intervals: A list of all snapshot intervals that should be considered.
        start: Start of the interval.
        end: End of the interval.
        latest: The latest datetime to use for non-incremental queries.
        restatements: A set of snapshot names being restated

    Returns:
        A dict containing all snapshots needing to be run with their associated interval params.
    """
    restatements = restatements or set()
    start_dt = to_datetime(start)

    snapshots_to_batches = {}

    for snapshot in Snapshot.hydrate_with_intervals_by_version(snapshots, intervals, is_dev=is_dev):
        model_start_dt = max(start_date(snapshot, snapshots) or start_dt, start_dt)
        snapshots_to_batches[snapshot] = [
            (to_datetime(s), to_datetime(e))
            for s, e in snapshot.missing_intervals(
                model_start_dt, end, latest, restatements=restatements
            )
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
    if snapshot.start:
        return snapshot.start

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

    if earliest:
        snapshot._start = earliest

    return earliest


def earliest_start_date(snapshots: t.Iterable[Snapshot]) -> datetime:
    """Get the earliest start date from a collection of snapshots.

    Args:
        snapshots: Snapshots to find earliest start date.
    Returns:
        The earliest start date or yesterday if none is found.
    """
    snapshots = list(snapshots)
    earliest = to_datetime(yesterday().date())
    if snapshots:
        for snapshot in snapshots:
            if not snapshot.parents and not snapshot.start:
                snapshot._start = earliest
        return min(start_date(snapshot, snapshots) or earliest for snapshot in snapshots)
    return earliest


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
