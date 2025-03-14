from __future__ import annotations
from enum import Enum
import logging
import typing as t
from sqlglot import exp
from sqlmesh.core import constants as c
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.environment import EnvironmentNamingInfo, execute_environment_statements
from sqlmesh.core.macros import RuntimeStage
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.notification_target import (
    NotificationEvent,
    NotificationTargetManager,
)
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotId,
    SnapshotEvaluator,
    apply_auto_restatements,
    earliest_start_date,
    missing_intervals,
    merge_intervals,
    snapshots_to_dag,
    Intervals,
)
from sqlmesh.core.snapshot.definition import (
    Interval,
    expand_range,
    parent_snapshots_by_name,
)
from sqlmesh.core.state_sync import StateSync
from sqlmesh.utils.concurrency import concurrent_apply_to_dag, NodeExecutionFailedError
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import (
    TimeLike,
    now_timestamp,
    to_timestamp,
    validate_date_range,
)
from sqlmesh.utils.errors import AuditError, NodeAuditsErrors, CircuitBreakerError, SQLMeshError

logger = logging.getLogger(__name__)
SnapshotToIntervals = t.Dict[Snapshot, Intervals]
# we store snapshot name instead of snapshots/snapshotids because pydantic
# is extremely slow to hash. snapshot names should be unique within a dag run
SchedulingUnit = t.Tuple[str, t.Tuple[Interval, int]]


class CompletionStatus(Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    NOTHING_TO_DO = "nothing_to_do"

    @property
    def is_success(self) -> bool:
        return self == CompletionStatus.SUCCESS

    @property
    def is_failure(self) -> bool:
        return self == CompletionStatus.FAILURE

    @property
    def is_nothing_to_do(self) -> bool:
        return self == CompletionStatus.NOTHING_TO_DO


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
        default_catalog: t.Optional[str],
        max_workers: int = 1,
        console: t.Optional[Console] = None,
        notification_target_manager: t.Optional[NotificationTargetManager] = None,
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

    def merged_missing_intervals(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        restatements: t.Optional[t.Dict[SnapshotId, Interval]] = None,
        interval_end_per_model: t.Optional[t.Dict[str, int]] = None,
        ignore_cron: bool = False,
        end_bounded: bool = False,
        selected_snapshots: t.Optional[t.Set[str]] = None,
    ) -> SnapshotToIntervals:
        """Find the largest contiguous date interval parameters based only on what is missing.

        For each node name, find all dependencies and look for a stored snapshot from the metastore. If a snapshot is found,
        calculate the missing intervals that need to be processed given the passed in start and end intervals.

        This is a superset of what may actually get processed at runtime based on things like batch size, signal readiness, etc.

        Args:
            start: The start of the run. Defaults to the min node start date.
            end: The end of the run. Defaults to now.
            execution_time: The date/time reference to use for execution time. Defaults to now.
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
        snapshots_to_intervals = compute_interval_params(
            snapshots,
            start=start or earliest_start_date(snapshots),
            end=end or now_timestamp(),
            deployability_index=deployability_index,
            execution_time=execution_time or now_timestamp(),
            restatements=restatements,
            interval_end_per_model=interval_end_per_model,
            ignore_cron=ignore_cron,
            end_bounded=end_bounded,
        )
        # Filtering snapshots after computing missing intervals because we need all snapshots in order
        # to correctly infer start dates.
        if selected_snapshots is not None:
            snapshots_to_intervals = {
                s: i for s, i in snapshots_to_intervals.items() if s.name in selected_snapshots
            }
        return snapshots_to_intervals

    def evaluate(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        execution_time: TimeLike,
        deployability_index: DeployabilityIndex,
        batch_index: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
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
            auto_restatement_enabled: Whether to enable auto restatements.
            kwargs: Additional kwargs to pass to the renderer.
        """
        validate_date_range(start, end)

        snapshots = parent_snapshots_by_name(snapshot, self.snapshots)

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
            snapshots=snapshots,
            deployability_index=deployability_index,
            wap_id=wap_id,
            **kwargs,
        )
        get_console().store_evaluation_audit_results(snapshot, audit_results)

        audit_errors_to_raise: t.List[AuditError] = []
        for audit_result in (result for result in audit_results if result.count):
            error = AuditError(
                audit_name=audit_result.audit.name,
                audit_args=audit_result.audit_args,
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
            if audit_result.blocking:
                audit_errors_to_raise.append(error)
            else:
                display_name = snapshot.display_name(
                    environment_naming_info,
                    default_catalog,
                    self.snapshot_evaluator.adapter.dialect,
                )
                get_console().log_warning(
                    f"\n{display_name}: {error}.",
                    long_message=f"{error}. Audit query:\n{error.query.sql(error.adapter_dialect)}",
                )

        if audit_errors_to_raise:
            raise NodeAuditsErrors(audit_errors_to_raise)

        self.state_sync.add_interval(snapshot, start, end, is_dev=not is_deployable)

    def run(
        self,
        environment: str | EnvironmentNamingInfo,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        restatements: t.Optional[t.Dict[SnapshotId, Interval]] = None,
        interval_end_per_model: t.Optional[t.Dict[str, int]] = None,
        ignore_cron: bool = False,
        end_bounded: bool = False,
        selected_snapshots: t.Optional[t.Set[str]] = None,
        circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        auto_restatement_enabled: bool = False,
        run_environment_statements: bool = False,
    ) -> CompletionStatus:
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
            auto_restatement_enabled: Whether to enable auto restatements.

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
            DeployabilityIndex.create(self.snapshots.values(), start=start)
            if environment_naming_info.name != c.PROD
            else DeployabilityIndex.all_deployable()
        )
        execution_time = execution_time or now_timestamp()

        self.state_sync.refresh_snapshot_intervals(self.snapshots.values())
        for s_id, interval in (restatements or {}).items():
            self.snapshots[s_id].remove_interval(interval)

        if auto_restatement_enabled:
            auto_restated_intervals = apply_auto_restatements(self.snapshots, execution_time)
            self.state_sync.add_snapshots_intervals(auto_restated_intervals)
            self.state_sync.update_auto_restatements(
                {s.name_version: s.next_auto_restatement_ts for s in self.snapshots.values()}
            )

        merged_intervals = self.merged_missing_intervals(
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

        if not merged_intervals:
            return CompletionStatus.NOTHING_TO_DO

        errors, skipped_intervals = self.run_merged_intervals(
            merged_intervals=merged_intervals,
            deployability_index=deployability_index,
            environment_naming_info=environment_naming_info,
            execution_time=execution_time,
            circuit_breaker=circuit_breaker,
            start=start,
            end=end,
            run_environment_statements=run_environment_statements,
        )

        self.console.stop_evaluation_progress(success=not errors)

        skipped_snapshots = {i[0] for i in skipped_intervals}
        self.console.log_skipped_models(skipped_snapshots)
        for skipped in skipped_snapshots:
            logger.info(f"SKIPPED snapshot {skipped}\n")

        for error in errors:
            if isinstance(error.__cause__, CircuitBreakerError):
                raise error.__cause__
            logger.info(str(error), exc_info=error)

        self.console.log_failed_models(errors)

        return CompletionStatus.FAILURE if errors else CompletionStatus.SUCCESS

    def batch_intervals(self, merged_intervals: SnapshotToIntervals) -> t.Dict[Snapshot, Intervals]:
        dag = snapshots_to_dag(merged_intervals)

        snapshot_intervals: t.Dict[SnapshotId, t.Tuple[Snapshot, t.List[Interval]]] = {
            snapshot.snapshot_id: (
                snapshot,
                [
                    i
                    for interval in intervals
                    for i in _expand_range_as_interval(*interval, snapshot.node.interval_unit)
                ],
            )
            for snapshot, intervals in merged_intervals.items()
        }
        snapshot_batches = {}
        all_unready_intervals: t.Dict[str, set[Interval]] = {}
        for snapshot_id in dag:
            if snapshot_id not in snapshot_intervals:
                continue
            snapshot, intervals = snapshot_intervals[snapshot_id]
            unready = set(intervals)
            intervals = snapshot.check_ready_intervals(intervals)
            unready -= set(intervals)

            for parent in snapshot.parents:
                if parent.name in all_unready_intervals:
                    unready.update(all_unready_intervals[parent.name])

            all_unready_intervals[snapshot.name] = unready

            batches = []
            batch_size = snapshot.node.batch_size
            next_batch: t.List[t.Tuple[int, int]] = []

            for interval in interval_diff(
                intervals, merge_intervals(unready), uninterrupted=snapshot.depends_on_past
            ):
                if (batch_size and len(next_batch) >= batch_size) or (
                    next_batch and interval[0] != next_batch[-1][-1]
                ):
                    batches.append((next_batch[0][0], next_batch[-1][-1]))
                    next_batch = []
                next_batch.append(interval)
            if next_batch:
                batches.append((next_batch[0][0], next_batch[-1][-1]))
            snapshot_batches[snapshot] = batches
        return snapshot_batches

    def run_merged_intervals(
        self,
        *,
        merged_intervals: SnapshotToIntervals,
        deployability_index: DeployabilityIndex,
        environment_naming_info: EnvironmentNamingInfo,
        execution_time: t.Optional[TimeLike] = None,
        circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        run_environment_statements: bool = False,
    ) -> t.Tuple[t.List[NodeExecutionFailedError[SchedulingUnit]], t.List[SchedulingUnit]]:
        """Runs precomputed batches of missing intervals.

        Args:
            merged_intervals: The snapshots and contiguous interval ranges to evaluate.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            environment_naming_info: The environment naming info the user is targeting when applying their change.
            execution_time: The date/time reference to use for execution time.
            circuit_breaker: An optional handler which checks if the run should be aborted.
            start: The start of the run.
            end: The end of the run.

        Returns:
            A tuple of errors and skipped intervals.
        """
        execution_time = execution_time or now_timestamp()

        batched_intervals = self.batch_intervals(merged_intervals)

        self.console.start_evaluation_progress(
            batched_intervals,
            environment_naming_info,
            self.default_catalog,
        )

        dag = self._dag(batched_intervals)

        snapshots_by_name = {snapshot.name: snapshot for snapshot in self.snapshots.values()}

        if run_environment_statements:
            environment_statements = self.state_sync.get_environment_statements(
                environment_naming_info.name
            )
            execute_environment_statements(
                adapter=self.snapshot_evaluator.adapter,
                environment_statements=environment_statements,
                runtime_stage=RuntimeStage.BEFORE_ALL,
                environment_naming_info=environment_naming_info,
                default_catalog=self.default_catalog,
                snapshots=snapshots_by_name,
                start=start,
                end=end,
                execution_time=execution_time,
            )

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
                self.evaluate(
                    snapshot=snapshot,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                    deployability_index=deployability_index,
                    batch_index=batch_idx,
                    environment_naming_info=environment_naming_info,
                    default_catalog=self.default_catalog,
                )
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
            if run_environment_statements:
                execute_environment_statements(
                    adapter=self.snapshot_evaluator.adapter,
                    environment_statements=environment_statements,
                    runtime_stage=RuntimeStage.AFTER_ALL,
                    environment_naming_info=environment_naming_info,
                    default_catalog=self.default_catalog,
                    snapshots=snapshots_by_name,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                )

            self.state_sync.recycle()

    def _dag(self, batches: SnapshotToIntervals) -> DAG[SchedulingUnit]:
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
        terminal_node = ((to_timestamp(0), to_timestamp(0)), -1)

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
    restatements: t.Optional[t.Dict[SnapshotId, Interval]] = None,
    interval_end_per_model: t.Optional[t.Dict[str, int]] = None,
    ignore_cron: bool = False,
    end_bounded: bool = False,
) -> SnapshotToIntervals:
    """Find the largest contiguous date interval parameters based only on what is missing.

    For each node name, find all dependencies and look for a stored snapshot from the metastore. If a snapshot is found,
    calculate the missing intervals that need to be processed given the passed in start and end intervals.

    This is a superset of what may actually get processed at runtime based on things like batch size, signal readiness, etc.

    Args:
        snapshots: A set of target snapshots for which intervals should be computed.
        start: Start of the interval.
        end: End of the interval.
        deployability_index: Determines snapshots that are deployable in the context of this evaluation.
        execution_time: The date/time reference to use for execution time.
        restatements: A dict of snapshot names being restated and their intervals.
        interval_end_per_model: The mapping from model FQNs to target end dates.
        ignore_cron: Whether to ignore the node's cron schedule.
        end_bounded: If set to true, the returned intervals will be bounded by the target end date, disregarding lookback,
            allow_partials, and other attributes that could cause the intervals to exceed the target end date.

    Returns:
        A dict containing all snapshots needing to be run with their associated interval params.
    """
    snapshot_merged_intervals = {}

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
        contiguous_batch = []
        next_batch: Intervals = []

        for interval in intervals:
            if next_batch and interval[0] != next_batch[-1][-1]:
                contiguous_batch.append((next_batch[0][0], next_batch[-1][-1]))
                next_batch = []
            next_batch.append(interval)
        if next_batch:
            contiguous_batch.append((next_batch[0][0], next_batch[-1][-1]))
        snapshot_merged_intervals[snapshot] = contiguous_batch

    return snapshot_merged_intervals


def interval_diff(
    intervals_a: Intervals, intervals_b: Intervals, uninterrupted: bool = False
) -> Intervals:
    if not intervals_a or not intervals_b:
        return intervals_a

    index_a, index_b = 0, 0
    len_a = len(intervals_a)
    len_b = len(intervals_b)

    results = []

    while index_a < len_a and index_b < len_b:
        interval_a = intervals_a[index_a]
        interval_b = intervals_b[index_b]

        if interval_a[0] >= interval_b[1]:
            index_b += 1
        elif interval_b[0] >= interval_a[1]:
            results.append(interval_a)
            index_a += 1
        else:
            if uninterrupted:
                return results

            if interval_a[0] >= interval_b[0]:
                index_a += 1
            else:
                index_b += 1

    if index_a < len_a:
        interval_a = intervals_a[index_a]
        if interval_a[0] >= interval_b[1] or interval_b[0] >= interval_a[1]:
            results.extend(intervals_a[index_a:])

    return results


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


def _expand_range_as_interval(
    start_ts: int, end_ts: int, interval_unit: IntervalUnit
) -> t.List[Interval]:
    values = expand_range(start_ts, end_ts, interval_unit)
    return [(values[i], values[i + 1]) for i in range(len(values) - 1)]
