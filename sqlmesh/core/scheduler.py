from __future__ import annotations

import abc
import logging
import traceback
import typing as t
from datetime import datetime

from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.notification_target import (
    NotificationEvent,
    NotificationTargetManager,
)
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotEvaluator,
    earliest_start_date,
    missing_intervals,
)
from sqlmesh.core.snapshot.definition import Interval as SnapshotInterval
from sqlmesh.core.snapshot.definition import SnapshotId
from sqlmesh.core.state_sync import StateSync
from sqlmesh.utils import format_exception
from sqlmesh.utils.concurrency import concurrent_apply_to_dag, NodeExecutionFailedError
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import (
    TimeLike,
    now,
    now_timestamp,
    to_datetime,
    to_timestamp,
    validate_date_range,
)
from sqlmesh.utils.errors import AuditError, CircuitBreakerError, SQLMeshError

logger = logging.getLogger(__name__)
Interval = t.Tuple[datetime, datetime]
Batch = t.List[Interval]
SnapshotToBatches = t.Dict[Snapshot, Batch]
# we store snapshot name instead of snapshots/snapshotids because pydantic
# is extremely slow to hash. snapshot names should be unique within a dag run
SchedulingUnit = t.Tuple[str, t.Tuple[Interval, int]]


class Signal(abc.ABC):
    @abc.abstractmethod
    def check_intervals(self, batch: Batch) -> t.Union[bool, Batch]:
        """Returns which intervals are ready from a list of scheduled intervals.

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


SignalFactory = t.Callable[[t.Dict[str, t.Union[str, int, float, bool]]], Signal]
_registered_signal_factory: t.Optional[SignalFactory] = None


def signal_factory(f: SignalFactory) -> None:
    """Specifies a function as the SignalFactory to use for building Signal instances from model signal metadata.

    Only one such function may be decorated with this decorator.

    Example:
        import typing as t
        from sqlmesh.core.scheduler import signal_factory, Batch, Signal

        class AlwaysReadySignal(Signal):
            def check_intervals(self, batch: Batch) -> t.Union[bool, Batch]:
                return True

        @signal_factory
        def my_signal_factory(signal_metadata: t.Dict[str, t.Union[str, int, float, bool]]) -> Signal:
            return AlwaysReadySignal()
    """

    global _registered_signal_factory

    if _registered_signal_factory is not None and _registered_signal_factory.__code__ != f.__code__:
        raise SQLMeshError("Only one function may be decorated with @signal_factory")

    _registered_signal_factory = f


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
        signal_factory: A factory method for building Signal instances from model signal configuration.
    """

    def __init__(
        self,
        snapshots: t.Iterable[Snapshot],
        snapshot_evaluator: SnapshotEvaluator,
        state_sync: StateSync,
        default_catalog: t.Optional[str],
        max_workers: int = 1,
        console: t.Optional[Console] = None,
        notification_target_manager: t.Optional[NotificationTargetManager] = None,
        signal_factory: t.Optional[SignalFactory] = None,
    ):
        self.state_sync = state_sync
        self.snapshots = {s.snapshot_id: s for s in snapshots}
        self.snapshot_per_version = _resolve_one_snapshot_per_version(self.snapshots.values())
        self.default_catalog = default_catalog
        self.snapshot_evaluator = snapshot_evaluator
        self.max_workers = max_workers
        self.console = console or get_console()
        self.notification_target_manager = (
            notification_target_manager or NotificationTargetManager()
        )
        self.signal_factory = signal_factory or _registered_signal_factory

    def batches(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        restatements: t.Optional[t.Dict[SnapshotId, SnapshotInterval]] = None,
        interval_end_per_model: t.Optional[t.Dict[str, int]] = None,
        ignore_cron: bool = False,
        end_bounded: bool = False,
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
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            restatements: A set of snapshot names being restated.
            interval_end_per_model: The mapping from model FQNs to target end dates.
            ignore_cron: Whether to ignore the node's cron schedule.
            end_bounded: If set to true, the returned intervals will be bounded by the target end date, disregarding lookback,
                allow_partials, and other attributes that could cause the intervals to exceed the target end date.
            selected_snapshots: A set of snapshot names to run. If not provided, all snapshots will be run.
        """
        restatements = restatements or {}
        validate_date_range(start, end)

        snapshots: t.Collection[Snapshot] = self.snapshot_per_version.values()
        if selected_snapshots is not None:
            snapshots = [s for s in snapshots if s.name in selected_snapshots]

        self.state_sync.refresh_snapshot_intervals(snapshots)

        return compute_interval_params(
            snapshots,
            start=start or earliest_start_date(snapshots),
            end=end or now(),
            deployability_index=deployability_index,
            execution_time=execution_time or now(),
            restatements=restatements,
            interval_end_per_model=interval_end_per_model,
            ignore_cron=ignore_cron,
            end_bounded=end_bounded,
            signal_factory=self.signal_factory,
        )

    def evaluate(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        deployability_index: DeployabilityIndex,
        batch_index: int,
        **kwargs: t.Any,
    ) -> None:
        """Evaluate a snapshot and add the processed interval to the state sync.

        Args:
            snapshot: Snapshot to evaluate.
            start: The start datetime to render.
            end: The end datetime to render.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            batch_index: If the snapshot is part of a batch of related snapshots; which index in the batch is it
            kwargs: Additional kwargs to pass to the renderer.
        """
        validate_date_range(start, end)

        snapshots = {
            self.snapshots[p_sid].name: self.snapshots[p_sid] for p_sid in snapshot.parents
        }
        snapshots[snapshot.name] = snapshot

        is_deployable = deployability_index.is_deployable(snapshot)

        wap_id = self.snapshot_evaluator.evaluate(
            snapshot,
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            deployability_index=deployability_index,
            batch_index=batch_index,
            **kwargs,
        )
        audit_results = self.snapshot_evaluator.audit(
            snapshot=snapshot,
            start=start,
            end=end,
            execution_time=execution_time,
            raise_exception=False,
            snapshots=snapshots,
            deployability_index=deployability_index,
            wap_id=wap_id,
            **kwargs,
        )

        audit_error_to_raise: t.Optional[AuditError] = None
        for audit_result in (result for result in audit_results if result.count):
            error = AuditError(
                audit_name=audit_result.audit.name,
                model=snapshot.model_or_none,
                count=t.cast(int, audit_result.count),
                query=t.cast(exp.Query, audit_result.query),
                adapter_dialect=self.snapshot_evaluator.adapter.dialect,
            )
            self.notification_target_manager.notify(NotificationEvent.AUDIT_FAILURE, error)
            if is_deployable and snapshot.node.owner:
                self.notification_target_manager.notify_user(
                    NotificationEvent.AUDIT_FAILURE, snapshot.node.owner, error
                )
            if audit_result.audit.blocking:
                audit_error_to_raise = error

        if audit_error_to_raise:
            logger.error(f"Audit Failure: {traceback.format_exc()}")
            raise audit_error_to_raise

        self.state_sync.add_interval(snapshot, start, end, is_dev=not is_deployable)

    def run(
        self,
        environment: str | EnvironmentNamingInfo,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        restatements: t.Optional[t.Dict[SnapshotId, SnapshotInterval]] = None,
        interval_end_per_model: t.Optional[t.Dict[str, int]] = None,
        ignore_cron: bool = False,
        end_bounded: bool = False,
        selected_snapshots: t.Optional[t.Set[str]] = None,
        circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
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
            interval_end_per_model: The mapping from model FQNs to target end dates.
            ignore_cron: Whether to ignore the node's cron schedule.
            end_bounded: If set to true, the evaluated intervals will be bounded by the target end date, disregarding lookback,
                allow_partials, and other attributes that could cause the intervals to exceed the target end date.
            selected_snapshots: A set of snapshot names to run. If not provided, all snapshots will be run.
            circuit_breaker: An optional handler which checks if the run should be aborted.
            deployability_index: Determines snapshots that are deployable in the context of this render.

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

        deployability_index = deployability_index or (
            DeployabilityIndex.create(self.snapshots.values())
            if environment_naming_info.name != c.PROD
            else DeployabilityIndex.all_deployable()
        )
        execution_time = execution_time or now()
        batches = self.batches(
            start,
            end,
            execution_time,
            deployability_index=deployability_index,
            restatements=restatements,
            interval_end_per_model=interval_end_per_model,
            ignore_cron=ignore_cron,
            end_bounded=end_bounded,
            selected_snapshots=selected_snapshots,
        )
        if not batches:
            return True

        self.console.start_evaluation_progress(
            {snapshot: len(intervals) for snapshot, intervals in batches.items()},
            environment_naming_info,
            self.default_catalog,
        )

        errors, skipped_intervals = self.run_batches(
            batches=batches,
            deployability_index=deployability_index,
            execution_time=execution_time,
            circuit_breaker=circuit_breaker,
        )

        self.console.stop_evaluation_progress(success=not errors)

        skipped_snapshots = {i[0] for i in skipped_intervals}
        for skipped in skipped_snapshots:
            log_message = f"SKIPPED snapshot {skipped}\n"
            self.console.log_status_update(log_message)
            logger.info(log_message)

        for error in errors:
            if isinstance(error.__cause__, CircuitBreakerError):
                raise error.__cause__
            sid = error.node[0]
            formatted_exception = "".join(format_exception(error.__cause__ or error))
            log_message = f"FAILED processing snapshot {sid}\n{formatted_exception}"
            self.console.log_error(log_message)
            # Log with INFO level to prevent duplicate messages in the console.
            logger.info(log_message)

        return not errors

    def run_batches(
        self,
        batches: SnapshotToBatches,
        deployability_index: DeployabilityIndex,
        execution_time: t.Optional[TimeLike] = None,
        circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
    ) -> t.Tuple[t.List[NodeExecutionFailedError[SchedulingUnit]], t.List[SchedulingUnit]]:
        """Runs precomputed batches of missing intervals.

        Args:
            batches: The batches of snapshots and intervals to evaluate.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            execution_time: The date/time time reference to use for execution time.
            circuit_breaker: An optional handler which checks if the run should be aborted.

        Returns:
            A tuple of errors and skipped intervals.
        """
        execution_time = execution_time or now()
        dag = self._dag(batches)

        snapshots_by_name = {snapshot.name: snapshot for snapshot in self.snapshots.values()}

        def evaluate_node(node: SchedulingUnit) -> None:
            if circuit_breaker and circuit_breaker():
                raise CircuitBreakerError()

            snapshot_name, ((start, end), batch_idx) = node
            if batch_idx == -1:
                return
            snapshot = snapshots_by_name[snapshot_name]

            self.console.start_snapshot_evaluation_progress(snapshot)

            execution_start_ts = now_timestamp()
            evaluation_duration_ms: t.Optional[int] = None

            try:
                assert execution_time  # mypy
                assert deployability_index  # mypy
                self.evaluate(snapshot, start, end, execution_time, deployability_index, batch_idx)
                evaluation_duration_ms = now_timestamp() - execution_start_ts
            finally:
                self.console.update_snapshot_evaluation_progress(
                    snapshot, batch_idx, evaluation_duration_ms
                )

        try:
            with self.snapshot_evaluator.concurrent_context():
                return concurrent_apply_to_dag(
                    dag,
                    evaluate_node,
                    self.max_workers,
                    raise_on_error=False,
                )
        finally:
            self.state_sync.recycle()

    def _dag(self, batches: SnapshotToBatches) -> DAG[SchedulingUnit]:
        """Builds a DAG of snapshot intervals to be evaluated.

        Args:
            batches: The batches of snapshots and intervals to evaluate.

        Returns:
            A DAG of snapshot intervals to be evaluated.
        """

        intervals_per_snapshot = {
            snapshot.name: intervals for snapshot, intervals in batches.items()
        }

        dag = DAG[SchedulingUnit]()
        terminal_node = ((to_datetime(0), to_datetime(0)), -1)

        for snapshot, intervals in batches.items():
            if not intervals:
                continue

            upstream_dependencies = []

            for p_sid in snapshot.parents:
                if p_sid in self.snapshots:
                    p_intervals = intervals_per_snapshot.get(p_sid.name, [])

                    if len(p_intervals) > 1:
                        upstream_dependencies.append((p_sid.name, terminal_node))
                    else:
                        for i, interval in enumerate(p_intervals):
                            upstream_dependencies.append((p_sid.name, (interval, i)))

            batch_concurrency = snapshot.node.batch_concurrency
            if snapshot.depends_on_past:
                batch_concurrency = 1

            for i, interval in enumerate(intervals):
                node = (snapshot.name, (interval, i))
                dag.add(node, upstream_dependencies)

                if len(intervals) > 1:
                    dag.add((snapshot.name, terminal_node), [node])

                if batch_concurrency and i >= batch_concurrency:
                    batch_idx_to_wait_for = i - batch_concurrency
                    dag.add(
                        node,
                        [
                            (
                                snapshot.name,
                                (intervals[batch_idx_to_wait_for], batch_idx_to_wait_for),
                            )
                        ],
                    )
        return dag


def compute_interval_params(
    snapshots: t.Collection[Snapshot],
    *,
    start: TimeLike,
    end: TimeLike,
    deployability_index: t.Optional[DeployabilityIndex] = None,
    execution_time: t.Optional[TimeLike] = None,
    restatements: t.Optional[t.Dict[SnapshotId, SnapshotInterval]] = None,
    interval_end_per_model: t.Optional[t.Dict[str, int]] = None,
    ignore_cron: bool = False,
    end_bounded: bool = False,
    signal_factory: t.Optional[SignalFactory] = None,
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
        deployability_index: Determines snapshots that are deployable in the context of this evaluation.
        execution_time: The date/time time reference to use for execution time.
        restatements: A dict of snapshot names being restated and their intervals.
        interval_end_per_model: The mapping from model FQNs to target end dates.
        ignore_cron: Whether to ignore the node's cron schedule.
        end_bounded: If set to true, the returned intervals will be bounded by the target end date, disregarding lookback,
            allow_partials, and other attributes that could cause the intervals to exceed the target end date.

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
        deployability_index=deployability_index,
        interval_end_per_model=interval_end_per_model,
        ignore_cron=ignore_cron,
        end_bounded=end_bounded,
    ).items():
        if signal_factory and snapshot.is_model:
            for signal in snapshot.model.render_signals(
                start=start, end=end, execution_time=execution_time
            ):
                intervals = _check_ready_intervals(
                    signal=signal_factory(signal),
                    intervals=intervals,
                )

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


def _contiguous_intervals(
    intervals: t.List[SnapshotInterval],
) -> t.List[t.List[SnapshotInterval]]:
    """Given a list of intervals with gaps, returns a list of sequences of contiguous intervals."""
    contiguous_intervals = []
    current_batch: t.List[SnapshotInterval] = []
    for interval in intervals:
        if len(current_batch) == 0 or interval[0] == current_batch[-1][-1]:
            current_batch.append(interval)
        else:
            contiguous_intervals.append(current_batch)
            current_batch = [interval]

    if len(current_batch) > 0:
        contiguous_intervals.append(current_batch)

    return contiguous_intervals


def _check_ready_intervals(
    signal: Signal,
    intervals: t.List[SnapshotInterval],
) -> t.List[SnapshotInterval]:
    """Returns a list of intervals that are considered ready by the provided signal.

    Note that this will handle gaps in the provided intervals. The returned intervals
    may introduce new gaps.
    """
    checked_intervals = []
    for interval_batch in _contiguous_intervals(intervals):
        batch = [(to_datetime(start), to_datetime(end)) for start, end in interval_batch]

        ready_intervals = signal.check_intervals(batch=batch)
        if isinstance(ready_intervals, bool):
            if not ready_intervals:
                batch = []
        elif isinstance(ready_intervals, list):
            for i in ready_intervals:
                if i not in batch:
                    raise RuntimeError(f"Signal returned unknown interval {i}")
            batch = ready_intervals
        else:
            raise ValueError(
                f"unexpected return value from signal, expected bool | list, got {type(ready_intervals)}"
            )

        checked_intervals.extend([(to_timestamp(start), to_timestamp(end)) for start, end in batch])
    return checked_intervals
