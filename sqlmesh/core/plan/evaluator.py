"""
# PlanEvaluator

A plan evaluator is responsible for evaluating a plan when it is being applied.

# Evaluation steps

At a high level, when a plan is evaluated, SQLMesh will:
- Push new snapshots to the state sync.
- Create snapshot tables.
- Backfill data.
- Promote the snapshots.

Refer to `sqlmesh.core.plan`.
"""

import abc
import logging
import typing as t

from sqlmesh.core import analytics
from sqlmesh.core import constants as c
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.notification_target import (
    NotificationTarget,
)
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotEvaluator,
    SnapshotIntervals,
    SnapshotId,
)
from sqlmesh.core.state_sync import StateSync
from sqlmesh.core.state_sync.base import PromotionResult
from sqlmesh.core.user import User
from sqlmesh.schedulers.airflow import common as airflow_common
from sqlmesh.schedulers.airflow.client import AirflowClient, BaseAirflowClient
from sqlmesh.schedulers.airflow.mwaa_client import MWAAClient
from sqlmesh.utils.errors import SQLMeshError

logger = logging.getLogger(__name__)


class PlanEvaluator(abc.ABC):
    @abc.abstractmethod
    def evaluate(
        self, plan: EvaluatablePlan, circuit_breaker: t.Optional[t.Callable[[], bool]] = None
    ) -> None:
        """Evaluates a plan by pushing snapshots and backfilling data.

        Given a plan, it pushes snapshots into the state and then kicks off
        the backfill process for all affected snapshots. Once backfill is done,
        snapshots that are part of the plan are promoted in the environment targeted
        by this plan.

        Args:
            plan: The plan to evaluate.
        """


class BuiltInPlanEvaluator(PlanEvaluator):
    def __init__(
        self,
        state_sync: StateSync,
        snapshot_evaluator: SnapshotEvaluator,
        create_scheduler: t.Callable[[t.Iterable[Snapshot]], Scheduler],
        default_catalog: t.Optional[str],
        console: t.Optional[Console] = None,
    ):
        self.state_sync = state_sync
        self.snapshot_evaluator = snapshot_evaluator
        self.create_scheduler = create_scheduler
        self.default_catalog = default_catalog
        self.console = console or get_console()

    def evaluate(
        self,
        plan: EvaluatablePlan,
        circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
    ) -> None:
        self.console.start_plan_evaluation(plan)
        analytics.collector.on_plan_apply_start(
            plan=plan,
            engine_type=self.snapshot_evaluator.adapter.dialect,
            state_sync_type=self.state_sync.state_type(),
            scheduler_type=c.BUILTIN,
        )

        try:
            new_snapshots = {s.snapshot_id: s for s in plan.new_snapshots}
            stored_snapshots = self.state_sync.get_snapshots(plan.environment.snapshots)
            snapshots = {**new_snapshots, **stored_snapshots}
            snapshots_by_name = {s.name: s for s in snapshots.values()}

            all_names = {name for name in snapshots_by_name if plan.is_selected_for_backfill(name)}

            deployability_index_for_evaluation = DeployabilityIndex.create(snapshots)
            deployability_index_for_creation = deployability_index_for_evaluation
            if plan.is_dev:
                before_promote_snapshots = all_names
                after_promote_snapshots = set()
            else:
                before_promote_snapshots = {
                    s.name
                    for s in snapshots.values()
                    if deployability_index_for_evaluation.is_representative(s)
                    and plan.is_selected_for_backfill(s.name)
                }
                after_promote_snapshots = all_names - before_promote_snapshots
                deployability_index_for_evaluation = DeployabilityIndex.all_deployable()

            self._push(plan, snapshots, deployability_index_for_creation)
            update_intervals_for_new_snapshots(plan.new_snapshots, self.state_sync)
            self._restate(plan, snapshots_by_name)
            self._backfill(
                plan,
                snapshots_by_name,
                before_promote_snapshots,
                deployability_index_for_evaluation,
                circuit_breaker=circuit_breaker,
            )
            promotion_result = self._promote(plan, snapshots, before_promote_snapshots)
            self._backfill(
                plan,
                snapshots_by_name,
                after_promote_snapshots,
                deployability_index_for_evaluation,
                circuit_breaker=circuit_breaker,
            )
            self._update_views(
                plan, snapshots, promotion_result, deployability_index_for_evaluation
            )

            if not plan.requires_backfill:
                self.console.log_success("Virtual Update executed successfully")
        except Exception as e:
            analytics.collector.on_plan_apply_end(plan_id=plan.plan_id, error=e)
            raise
        else:
            analytics.collector.on_plan_apply_end(plan_id=plan.plan_id)
        finally:
            self.console.stop_plan_evaluation()

    def _backfill(
        self,
        plan: EvaluatablePlan,
        snapshots_by_name: t.Dict[str, Snapshot],
        selected_snapshots: t.Set[str],
        deployability_index: DeployabilityIndex,
        circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
    ) -> None:
        """Backfill missing intervals for snapshots that are part of the given plan.

        Args:
            plan: The plan to source snapshots from.
            selected_snapshots: The snapshots to backfill.
        """
        if not plan.requires_backfill or not selected_snapshots:
            return

        scheduler = self.create_scheduler(snapshots_by_name.values())
        is_run_successful = scheduler.run(
            plan.environment,
            plan.start,
            plan.end,
            execution_time=plan.execution_time,
            restatements={
                snapshots_by_name[name].snapshot_id: interval
                for name, interval in plan.restatements.items()
            },
            selected_snapshots=selected_snapshots,
            deployability_index=deployability_index,
            circuit_breaker=circuit_breaker,
            end_bounded=plan.end_bounded,
            interval_end_per_model=plan.interval_end_per_model,
        )
        if not is_run_successful:
            raise SQLMeshError("Plan application failed.")

    def _push(
        self,
        plan: EvaluatablePlan,
        snapshots: t.Dict[SnapshotId, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex] = None,
    ) -> None:
        """Push the snapshots to the state sync.

        As a part of plan pushing, snapshot tables are created.

        Args:
            plan: The plan to source snapshots from.
            deployability_index: Indicates which snapshots are deployable in the context of this creation.
        """
        snapshots_to_create = [
            s
            for s in snapshots.values()
            if s.is_model and not s.is_symbolic and plan.is_selected_for_backfill(s.name)
        ]
        snapshots_to_create_count = len(snapshots_to_create)

        if snapshots_to_create_count > 0:
            self.console.start_creation_progress(
                snapshots_to_create_count, plan.environment, self.default_catalog
            )

        completed = False
        try:
            self.snapshot_evaluator.create(
                snapshots_to_create,
                snapshots,
                allow_destructive_snapshots=plan.allow_destructive_models,
                deployability_index=deployability_index,
                on_complete=self.console.update_creation_progress,
            )
            completed = True
        finally:
            self.console.stop_creation_progress(success=completed)

        self.state_sync.push_snapshots(plan.new_snapshots)

        analytics.collector.on_snapshots_created(
            new_snapshots=plan.new_snapshots, plan_id=plan.plan_id
        )

    def _promote(
        self,
        plan: EvaluatablePlan,
        snapshots: t.Dict[SnapshotId, Snapshot],
        no_gaps_snapshot_names: t.Optional[t.Set[str]] = None,
    ) -> PromotionResult:
        """Promote a plan.

        Args:
            plan: The plan to promote.
            no_gaps_snapshot_names: The names of snapshots to check for gaps if the no gaps check is enabled in the plan.
            If not provided, all snapshots are checked.
        """
        promotion_result = self.state_sync.promote(
            plan.environment,
            no_gaps_snapshot_names=no_gaps_snapshot_names if plan.no_gaps else set(),
        )

        if not plan.is_dev:
            self.snapshot_evaluator.migrate(
                [s for s in snapshots.values() if s.is_paused],
                snapshots,
                plan.allow_destructive_models,
            )
            if not plan.ensure_finalized_snapshots:
                # Only unpause at this point if we don't have to use the finalized snapshots
                # for subsequent plan applications. Otherwise, unpause right before finalizing
                # the environment.
                self.state_sync.unpause_snapshots(promotion_result.added, plan.end)

        return promotion_result

    def _update_views(
        self,
        plan: EvaluatablePlan,
        snapshots: t.Dict[SnapshotId, Snapshot],
        promotion_result: PromotionResult,
        deployability_index: t.Optional[DeployabilityIndex] = None,
    ) -> None:
        """Update environment views.

        Args:
            plan: The plan to promote.
            promotion_result: The result of the promotion.
            deployability_index: Indicates which snapshots are deployable in the context of this promotion.
        """
        if not plan.is_dev and plan.ensure_finalized_snapshots:
            # Unpause right before finalizing the environment in case when
            # we need to use the finalized snapshots for subsequent plan applications.
            # Otherwise, unpause right after updatig the environment record.
            self.state_sync.unpause_snapshots(promotion_result.added, plan.end)

        environment = plan.environment

        self.console.start_promotion_progress(
            len(promotion_result.added) + len(promotion_result.removed),
            environment.naming_info,
            self.default_catalog,
        )

        completed = False
        try:
            self.snapshot_evaluator.promote(
                [snapshots[s.snapshot_id] for s in promotion_result.added],
                environment.naming_info,
                deployability_index=deployability_index,
                on_complete=lambda s: self.console.update_promotion_progress(s, True),
            )
            if promotion_result.removed_environment_naming_info:
                self.snapshot_evaluator.demote(
                    promotion_result.removed,
                    promotion_result.removed_environment_naming_info,
                    on_complete=lambda s: self.console.update_promotion_progress(s, False),
                )
            self.state_sync.finalize(environment)
            completed = True
        finally:
            self.console.stop_promotion_progress(success=completed)

    def _restate(self, plan: EvaluatablePlan, snapshots_by_name: t.Dict[str, Snapshot]) -> None:
        if not plan.restatements:
            return

        self.state_sync.remove_intervals(
            [(snapshots_by_name[name], interval) for name, interval in plan.restatements.items()],
            remove_shared_versions=not plan.is_dev,
        )


class BaseAirflowPlanEvaluator(PlanEvaluator):
    def __init__(
        self,
        console: t.Optional[Console],
        blocking: bool,
        dag_run_poll_interval_secs: int,
        dag_creation_poll_interval_secs: int,
        dag_creation_max_retry_attempts: int,
    ):
        self.blocking = blocking
        self.dag_run_poll_interval_secs = dag_run_poll_interval_secs
        self.dag_creation_poll_interval_secs = dag_creation_poll_interval_secs
        self.dag_creation_max_retry_attempts = dag_creation_max_retry_attempts
        self.console = console or get_console()

    def evaluate(
        self, plan: EvaluatablePlan, circuit_breaker: t.Optional[t.Callable[[], bool]] = None
    ) -> None:
        plan_request_id = plan.plan_id
        self._apply_plan(plan, plan_request_id)

        analytics.collector.on_plan_apply_start(
            plan=plan,
            engine_type=None,
            state_sync_type=None,
            scheduler_type=c.AIRFLOW,
        )

        if self.blocking:
            plan_application_dag_id = airflow_common.plan_application_dag_id(
                plan.environment.name, plan_request_id
            )

            self.console.log_status_update(
                f"Waiting for the plan application DAG '{plan_application_dag_id}' to be provisioned on Airflow"
            )

            plan_application_dag_run_id = self.client.wait_for_first_dag_run(
                plan_application_dag_id,
                self.dag_creation_poll_interval_secs,
                self.dag_creation_max_retry_attempts,
            )

            self.client.print_tracking_url(
                plan_application_dag_id,
                plan_application_dag_run_id,
                "plan application",
            )
            plan_application_succeeded = self.client.wait_for_dag_run_completion(
                plan_application_dag_id,
                plan_application_dag_run_id,
                self.dag_run_poll_interval_secs,
            )
            if not plan_application_succeeded:
                raise SQLMeshError("Plan application failed.")

            self.console.log_success("The plan has been applied successfully")

    @property
    def client(self) -> BaseAirflowClient:
        raise NotImplementedError

    def _apply_plan(self, plan: EvaluatablePlan, plan_request_id: str) -> None:
        raise NotImplementedError


class StateBasedAirflowPlanEvaluator(BaseAirflowPlanEvaluator):
    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int
    notification_targets: t.Optional[t.List[NotificationTarget]]
    users: t.Optional[t.List[User]]

    def _apply_plan(self, plan: EvaluatablePlan, plan_request_id: str) -> None:
        from sqlmesh.schedulers.airflow.plan import PlanDagState, create_plan_dag_spec

        plan_application_request = airflow_common.PlanApplicationRequest(
            plan=plan,
            notification_targets=self.notification_targets or [],
            backfill_concurrent_tasks=self.backfill_concurrent_tasks,
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
            users=self.users or [],
        )
        plan_dag_spec = create_plan_dag_spec(plan_application_request, self.state_sync)
        PlanDagState.from_state_sync(self.state_sync).add_dag_spec(plan_dag_spec)

    @property
    def state_sync(self) -> StateSync:
        raise NotImplementedError


class AirflowPlanEvaluator(StateBasedAirflowPlanEvaluator):
    def __init__(
        self,
        airflow_client: AirflowClient,
        console: t.Optional[Console] = None,
        blocking: bool = True,
        dag_run_poll_interval_secs: int = 10,
        dag_creation_poll_interval_secs: int = 30,
        dag_creation_max_retry_attempts: int = 10,
        notification_targets: t.Optional[t.List[NotificationTarget]] = None,
        backfill_concurrent_tasks: int = 1,
        ddl_concurrent_tasks: int = 1,
        users: t.Optional[t.List[User]] = None,
        state_sync: t.Optional[StateSync] = None,
    ):
        super().__init__(
            console,
            blocking,
            dag_run_poll_interval_secs,
            dag_creation_poll_interval_secs,
            dag_creation_max_retry_attempts,
        )
        self._airflow_client = airflow_client
        self.notification_targets = notification_targets or []
        self.backfill_concurrent_tasks = backfill_concurrent_tasks
        self.ddl_concurrent_tasks = ddl_concurrent_tasks
        self.users = users or []

        self._state_sync = state_sync

    @property
    def client(self) -> BaseAirflowClient:
        return self._airflow_client

    @property
    def state_sync(self) -> StateSync:
        if self._state_sync is None:
            raise SQLMeshError("State Sync is not configured")
        return self._state_sync

    def _apply_plan(self, plan: EvaluatablePlan, plan_request_id: str) -> None:
        if self._state_sync is not None:
            super()._apply_plan(plan, plan_request_id)
            return

        self._airflow_client.apply_plan(
            plan,
            notification_targets=self.notification_targets,
            backfill_concurrent_tasks=self.backfill_concurrent_tasks,
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
            users=self.users,
        )


class MWAAPlanEvaluator(StateBasedAirflowPlanEvaluator):
    def __init__(
        self,
        client: MWAAClient,
        state_sync: StateSync,
        console: t.Optional[Console] = None,
        blocking: bool = True,
        dag_run_poll_interval_secs: int = 10,
        dag_creation_poll_interval_secs: int = 30,
        dag_creation_max_retry_attempts: int = 10,
        notification_targets: t.Optional[t.List[NotificationTarget]] = None,
        backfill_concurrent_tasks: int = 1,
        ddl_concurrent_tasks: int = 1,
        users: t.Optional[t.List[User]] = None,
    ):
        super().__init__(
            console,
            blocking,
            dag_run_poll_interval_secs,
            dag_creation_poll_interval_secs,
            dag_creation_max_retry_attempts,
        )
        self._mwaa_client = client
        self._state_sync = state_sync
        self.notification_targets = notification_targets or []
        self.backfill_concurrent_tasks = backfill_concurrent_tasks
        self.ddl_concurrent_tasks = ddl_concurrent_tasks
        self.users = users or []

    @property
    def client(self) -> BaseAirflowClient:
        return self._mwaa_client

    @property
    def state_sync(self) -> StateSync:
        return self._state_sync


def update_intervals_for_new_snapshots(
    snapshots: t.Collection[Snapshot], state_sync: StateSync
) -> None:
    snapshots_intervals: t.List[SnapshotIntervals] = []
    for snapshot in state_sync.refresh_snapshot_intervals(snapshots):
        if snapshot.is_forward_only:
            snapshot.dev_intervals = snapshot.intervals.copy()
            snapshot_intervals = snapshot.snapshot_intervals
            snapshot_intervals.intervals.clear()
            snapshots_intervals.append(snapshot_intervals)

    if snapshots_intervals:
        state_sync.add_snapshots_intervals(snapshots_intervals)
