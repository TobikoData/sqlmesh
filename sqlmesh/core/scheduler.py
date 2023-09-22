from __future__ import annotations

import logging
import typing as t
from datetime import datetime

from sqlmesh.core import constants as c
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.model import SeedModel
from sqlmesh.core.notification_target import (
    NotificationEvent,
    NotificationTargetManager,
)
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotEvaluator,
    SnapshotIdLike,
    earliest_start_date,
    missing_intervals,
)
from sqlmesh.core.snapshot.definition import Interval as SnapshotInterval
from sqlmesh.core.state_sync import StateSync
from sqlmesh.utils import format_exception
from sqlmesh.utils.concurrency import concurrent_apply_to_dag
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike, now, to_datetime, validate_date_range
from sqlmesh.utils.errors import AuditError, SQLMeshError

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
        snapshots: A collection of snapshots/ids.
        snapshot_evaluator: The snapshot evaluator to execute queries.
        state_sync: The state sync to pull saved snapshots.
        max_workers: The maximum number of parallel queries to run.
        console: The rich instance used for printing scheduling information.
    """

    def __init__(
        self,
        snapshots: t.Iterable[SnapshotIdLike],
        snapshot_evaluator: SnapshotEvaluator,
        state_sync: StateSync,
        max_workers: int = 1,
        console: t.Optional[Console] = None,
        notification_target_manager: t.Optional[NotificationTargetManager] = None,
    ):
        self.state_sync = state_sync
        self.snapshots = self.state_sync.get_snapshots(snapshots)
        self.snapshot_per_version = _resolve_one_snapshot_per_version(self.snapshots.values())
        self.snapshot_evaluator = snapshot_evaluator
        self.max_workers = max_workers
        self.console = console or get_console()
        self.notification_target_manager = (
            notification_target_manager or NotificationTargetManager()
        )

    def batches(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        is_dev: bool = False,
        restatements: t.Optional[t.Dict[str, SnapshotInterval]] = None,
        ignore_cron: bool = False,
        selected_snapshots: t.Optional[t.Set[str]] = None,
    ) -> SnapshotToBatches:
        """Find the optimal date interval paramaters based on what needs processing and maximal batch size.

        For each node name, find all dependencies and look for a stored snapshot from the metastore. If a snapshot is found,
        calculate the missing intervals that need to be processed given the passed in start and end intervals.

        If a snapshot's node specifies a batch size, consecutive intervals are merged into batches of a size that is less than
        or equal to the configured one. If no batch size is specified, then it uses the intervals that correspond to the node's cron expression.
        For example, if a node is supposed to run daily and has 70 days to backfill with a batch size set to 30, there would be 2 jobs
        with 30 days and 1 job with 10.

        Args:
            start: The start of the run. Defaults to the min node start date.
            end: The end of the run. Defaults to now.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            is_dev: Indicates whether the evaluation happens in the development mode and temporary
                tables / table clones should be used where applicable.
            restatements: A set of snapshot names being restated.
            ignore_cron: Whether to ignore the node's cron schedule.
            selected_snapshots: A set of snapshot names to run. If not provided, all snapshots will be run.
        """
        restatements = restatements or {}
        validate_date_range(start, end)

        snapshots: t.Collection[Snapshot] = self.snapshot_per_version.values()
        if selected_snapshots is not None:
            snapshots = [s for s in snapshots if s.name in selected_snapshots]

        return compute_interval_params(
            snapshots,
            start=start or earliest_start_date(snapshots),
            end=end or now(),
            is_dev=is_dev,
            execution_time=execution_time or now(),
            restatements=restatements,
            ignore_cron=ignore_cron,
        )

    def evaluate(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """Evaluate a snapshot and add the processed interval to the state sync.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            is_dev: Indicates whether the evaluation happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.
        """
        validate_date_range(start, end)

        snapshots = {
            **{p_sid.name: self.snapshots[p_sid] for p_sid in snapshot.parents},
            snapshot.name: snapshot,
        }

        if isinstance(snapshot.node, SeedModel) and not snapshot.node.is_hydrated:
            snapshot = self.state_sync.get_snapshots([snapshot], hydrate_seeds=True)[
                snapshot.snapshot_id
            ]

        self.snapshot_evaluator.evaluate(
            snapshot,
            start,
            end,
            execution_time,
            snapshots=snapshots,
            is_dev=is_dev,
            **kwargs,
        )
        try:
            self.snapshot_evaluator.audit(
                snapshot=snapshot,
                start=start,
                end=end,
                execution_time=execution_time,
                snapshots=snapshots,
                is_dev=is_dev,
                **kwargs,
            )
        except AuditError as e:
            self.notification_target_manager.notify(NotificationEvent.AUDIT_FAILURE, e)
            if not is_dev and snapshot.node.owner:
                self.notification_target_manager.notify_user(
                    NotificationEvent.AUDIT_FAILURE, snapshot.node.owner, e
                )
            raise e
        self.state_sync.add_interval(snapshot, start, end, is_dev=is_dev)

    def run(
        self,
        environment: str | EnvironmentNamingInfo,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        restatements: t.Optional[t.Dict[str, SnapshotInterval]] = None,
        ignore_cron: bool = False,
        selected_snapshots: t.Optional[t.Set[str]] = None,
    ) -> bool:
        """Concurrently runs all snapshots in topological order.

        Args:
            environment: The environment naming info the user is targeting when applying their change.
                Can just be the environment name if the user is targeting a remote environment and wants to get the remote
                naming info
            start: The start of the run. Defaults to the min node start date.
            end: The end of the run. Defaults to now.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            restatements: A dict of snapshots to restate and their intervals.
            ignore_cron: Whether to ignore the node's cron schedule.
            selected_snapshots: A set of snapshot names to run. If not provided, all snapshots will be run.

        Returns:
            True if the execution was successful and False otherwise.
        """
        restatements = restatements or {}
        validate_date_range(start, end)
        if isinstance(environment, str):
            env = self.state_sync.get_environment(environment)
            if not env:
                raise SQLMeshError(
                    "Was not provided an environment suffix target and the environment doesn't exist."
                    "Are you running for the first time and need to run plan/apply first?"
                )
            environment_naming_info = env.naming_info
        else:
            environment_naming_info = environment

        is_dev = environment_naming_info.name != c.PROD
        execution_time = execution_time or now()
        batches = self.batches(
            start,
            end,
            execution_time,
            is_dev=is_dev,
            restatements=restatements,
            ignore_cron=ignore_cron,
            selected_snapshots=selected_snapshots,
        )
        if not batches:
            return True

        dag = self._dag(batches)

        visited = set()
        for snapshot, _ in dag.sorted:
            if snapshot in visited:
                continue
            visited.add(snapshot)

        self.console.start_evaluation_progress(
            {snapshot: len(intervals) for snapshot, intervals in batches.items()},
            environment_naming_info,
        )

        def evaluate_node(node: SchedulingUnit) -> None:
            assert execution_time
            snapshot, (start, end) = node
            self.console.start_snapshot_evaluation_progress(snapshot)
            try:
                self.evaluate(snapshot, start, end, execution_time, is_dev=is_dev)
            finally:
                self.console.update_snapshot_evaluation_progress(snapshot, 1)

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

        self.console.stop_evaluation_progress(success=not errors)

        for error in errors:
            sid = error.node[0]
            formatted_exception = "".join(format_exception(error.__cause__ or error))
            self.console.log_error(f"FAILED processing snapshot {sid}\n{formatted_exception}")

        skipped_snapshots = {i[0] for i in skipped_intervals}
        for skipped in skipped_snapshots:
            self.console.log_status_update(f"SKIPPED snapshot {skipped}\n")

        return not errors

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
    snapshots: t.Collection[Snapshot],
    *,
    start: TimeLike,
    end: TimeLike,
    is_dev: bool,
    execution_time: t.Optional[TimeLike] = None,
    restatements: t.Optional[t.Dict[str, SnapshotInterval]] = None,
    ignore_cron: bool = False,
) -> SnapshotToBatches:
    """Find the optimal date interval paramaters based on what needs processing and maximal batch size.

    For each node name, find all dependencies and look for a stored snapshot from the metastore. If a snapshot is found,
    calculate the missing intervals that need to be processed given the passed in start and end intervals.

    If a snapshot's node specifies a batch size, consecutive intervals are merged into batches of a size that is less than
    or equal to the configured one. If no batch size is specified, then it uses the intervals that correspond to the node's cron expression.
    For example, if a node is supposed to run daily and has 70 days to backfill with a batch size set to 30, there would be 2 jobs
    with 30 days and 1 job with 10.

    Args:
        snapshots: A set of target snapshots for which intervals should be computed.
        intervals: A list of all snapshot intervals that should be considered.
        start: Start of the interval.
        end: End of the interval.
        is_dev: Whether or not these intervals are for development.
        execution_time: The date/time time reference to use for execution time.
        restatements: A dict of snapshot names being restated and their intervals.
        ignore_cron: Whether to ignore the node's cron schedule.

    Returns:
        A dict containing all snapshots needing to be run with their associated interval params.
    """
    snapshot_batches = {}

    for snapshot, intervals in missing_intervals(
        snapshots,
        start=start,
        end=end,
        execution_time=execution_time,
        restatements=restatements,
        is_dev=is_dev,
        ignore_cron=ignore_cron,
    ).items():
        batches = []
        batch_size = snapshot.node.batch_size
        next_batch: t.List[t.Tuple[int, int]] = []

        for interval in intervals:
            if (batch_size and len(next_batch) >= batch_size) or (
                next_batch and interval[0] != next_batch[-1][-1]
            ):
                batches.append((next_batch[0][0], next_batch[-1][-1]))
                next_batch = []
            next_batch.append(interval)
        if next_batch:
            batches.append((next_batch[0][0], next_batch[-1][-1]))
        snapshot_batches[snapshot] = [(to_datetime(s), to_datetime(e)) for s, e in batches]

    return snapshot_batches


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
