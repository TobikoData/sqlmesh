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
from sqlmesh.core.environment import EnvironmentNamingInfo, execute_environment_statements
from sqlmesh.core.macros import RuntimeStage
from sqlmesh.core.snapshot.definition import Interval, to_view_mapping
from sqlmesh.core.plan import stages
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotEvaluator,
    SnapshotIntervals,
    SnapshotId,
    SnapshotInfoLike,
    SnapshotTableInfo,
    SnapshotCreationFailedError,
)
from sqlmesh.utils import to_snake_case
from sqlmesh.core.state_sync import StateSync
from sqlmesh.utils.concurrency import NodeExecutionFailedError
from sqlmesh.utils.errors import PlanError, SQLMeshError
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import now

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
        self._circuit_breaker: t.Optional[t.Callable[[], bool]] = None

    def evaluate(
        self,
        plan: EvaluatablePlan,
        circuit_breaker: t.Optional[t.Callable[[], bool]] = None,
    ) -> None:
        self._circuit_breaker = circuit_breaker
        self.console.start_plan_evaluation(plan)
        analytics.collector.on_plan_apply_start(
            plan=plan,
            engine_type=self.snapshot_evaluator.adapter.dialect,
            state_sync_type=self.state_sync.state_type(),
            scheduler_type=c.BUILTIN,
        )

        try:
            plan_stages = stages.build_plan_stages(plan, self.state_sync, self.default_catalog)
            self._evaluate_stages(plan_stages, plan)
        except Exception as e:
            analytics.collector.on_plan_apply_end(plan_id=plan.plan_id, error=e)
            raise
        else:
            analytics.collector.on_plan_apply_end(plan_id=plan.plan_id)
        finally:
            self.console.stop_plan_evaluation()

    def _evaluate_stages(
        self, plan_stages: t.List[stages.PlanStage], plan: EvaluatablePlan
    ) -> None:
        for stage in plan_stages:
            stage_name = stage.__class__.__name__
            handler_name = f"visit_{to_snake_case(stage_name)}"
            if not hasattr(self, handler_name):
                raise SQLMeshError(f"Unexpected plan stage: {stage_name}")
            logger.info("Evaluating plan stage %s", stage_name)
            handler = getattr(self, handler_name)
            handler(stage, plan)

    def visit_before_all_stage(self, stage: stages.BeforeAllStage, plan: EvaluatablePlan) -> None:
        execute_environment_statements(
            adapter=self.snapshot_evaluator.adapter,
            environment_statements=stage.statements,
            runtime_stage=RuntimeStage.BEFORE_ALL,
            environment_naming_info=plan.environment.naming_info,
            default_catalog=self.default_catalog,
            snapshots=stage.all_snapshots,
            start=plan.start,
            end=plan.end,
            execution_time=plan.execution_time,
        )

    def visit_after_all_stage(self, stage: stages.AfterAllStage, plan: EvaluatablePlan) -> None:
        execute_environment_statements(
            adapter=self.snapshot_evaluator.adapter,
            environment_statements=stage.statements,
            runtime_stage=RuntimeStage.AFTER_ALL,
            environment_naming_info=plan.environment.naming_info,
            default_catalog=self.default_catalog,
            snapshots=stage.all_snapshots,
            start=plan.start,
            end=plan.end,
            execution_time=plan.execution_time,
        )

    def visit_create_snapshot_records_stage(
        self, stage: stages.CreateSnapshotRecordsStage, plan: EvaluatablePlan
    ) -> None:
        self.state_sync.push_snapshots(stage.snapshots)
        analytics.collector.on_snapshots_created(
            new_snapshots=stage.snapshots, plan_id=plan.plan_id
        )
        # Update the intervals for the new forward-only snapshots
        self._update_intervals_for_new_snapshots(stage.snapshots)

    def visit_physical_layer_update_stage(
        self, stage: stages.PhysicalLayerUpdateStage, plan: EvaluatablePlan
    ) -> None:
        skip_message = "" if plan.restatements else "\nSKIP: No physical layer updates to perform"

        snapshots_to_create = stage.snapshots
        if not snapshots_to_create:
            self.console.log_success(skip_message)
            return

        completion_status = None
        progress_stopped = False
        try:
            completion_status = self.snapshot_evaluator.create(
                snapshots_to_create,
                stage.all_snapshots,
                allow_destructive_snapshots=plan.allow_destructive_models,
                deployability_index=stage.deployability_index,
                on_start=lambda x: self.console.start_creation_progress(
                    x, plan.environment, self.default_catalog
                ),
                on_complete=self.console.update_creation_progress,
            )
            if completion_status.is_nothing_to_do:
                self.console.log_success(skip_message)
                return
        except SnapshotCreationFailedError as ex:
            self.console.stop_creation_progress(success=False)
            progress_stopped = True

            for error in ex.errors:
                logger.info(str(error), exc_info=error)

            self.console.log_skipped_models({s.name for s in ex.skipped})
            self.console.log_failed_models(ex.errors)

            raise PlanError("Plan application failed.")
        finally:
            if not progress_stopped:
                self.console.stop_creation_progress(
                    success=completion_status is not None and completion_status.is_success
                )

    def visit_backfill_stage(self, stage: stages.BackfillStage, plan: EvaluatablePlan) -> None:
        if plan.empty_backfill:
            intervals_to_add = []
            for snapshot in stage.all_snapshots.values():
                if not snapshot.evaluatable or not plan.is_selected_for_backfill(snapshot.name):
                    # Skip snapshots that are not evaluatable or not selected for backfill.
                    continue
                intervals = [
                    snapshot.inclusive_exclusive(plan.start, plan.end, strict=False, expand=False)
                ]
                is_deployable = stage.deployability_index.is_deployable(snapshot)
                intervals_to_add.append(
                    SnapshotIntervals(
                        name=snapshot.name,
                        identifier=snapshot.identifier,
                        version=snapshot.version,
                        dev_version=snapshot.dev_version,
                        intervals=intervals if is_deployable else [],
                        dev_intervals=intervals if not is_deployable else [],
                    )
                )
            self.state_sync.add_snapshots_intervals(intervals_to_add)
            self.console.log_success("SKIP: No model batches to execute")
            return

        if not stage.snapshot_to_intervals:
            self.console.log_success("SKIP: No model batches to execute")
            return

        scheduler = self.create_scheduler(stage.all_snapshots.values())
        errors, _ = scheduler.run_merged_intervals(
            merged_intervals=stage.snapshot_to_intervals,
            deployability_index=stage.deployability_index,
            environment_naming_info=plan.environment.naming_info,
            execution_time=plan.execution_time,
            circuit_breaker=self._circuit_breaker,
            start=plan.start,
            end=plan.end,
        )
        if errors:
            raise PlanError("Plan application failed.")

    def visit_audit_only_run_stage(
        self, stage: stages.AuditOnlyRunStage, plan: EvaluatablePlan
    ) -> None:
        audit_snapshots = stage.snapshots
        if not audit_snapshots:
            return

        # If there are any snapshots to be audited, we'll reuse the scheduler's internals to audit them
        scheduler = self.create_scheduler(audit_snapshots)
        completion_status = scheduler.audit(
            plan.environment,
            plan.start,
            plan.end,
            execution_time=plan.execution_time,
            end_bounded=plan.end_bounded,
            interval_end_per_model=plan.interval_end_per_model,
        )

        if completion_status.is_failure:
            raise PlanError("Plan application failed.")

    def visit_restatement_stage(
        self, stage: stages.RestatementStage, plan: EvaluatablePlan
    ) -> None:
        snapshot_intervals_to_restate = {(s, i) for s, i in stage.snapshot_intervals.items()}

        # Restating intervals on prod plans should mean that the intervals are cleared across
        # all environments, not just the version currently in prod
        # This ensures that work done in dev environments can still be promoted to prod
        # by forcing dev environments to re-run intervals that changed in prod
        #
        # Without this rule, its possible that promoting a dev table to prod will introduce old data to prod
        snapshot_intervals_to_restate.update(
            self._restatement_intervals_across_all_environments(
                prod_restatements=plan.restatements,
                disable_restatement_models=plan.disabled_restatement_models,
                loaded_snapshots={s.snapshot_id: s for s in stage.all_snapshots.values()},
            )
        )

        self.state_sync.remove_intervals(
            snapshot_intervals=list(snapshot_intervals_to_restate),
            remove_shared_versions=plan.is_prod,
        )

    def visit_environment_record_update_stage(
        self, stage: stages.EnvironmentRecordUpdateStage, plan: EvaluatablePlan
    ) -> None:
        self.state_sync.promote(
            plan.environment,
            no_gaps_snapshot_names=stage.no_gaps_snapshot_names if plan.no_gaps else set(),
            environment_statements=plan.environment_statements,
        )

    def visit_migrate_schemas_stage(
        self, stage: stages.MigrateSchemasStage, plan: EvaluatablePlan
    ) -> None:
        try:
            self.snapshot_evaluator.migrate(
                stage.snapshots,
                stage.all_snapshots,
                allow_destructive_snapshots=plan.allow_destructive_models,
                deployability_index=stage.deployability_index,
            )
        except NodeExecutionFailedError as ex:
            raise PlanError(str(ex.__cause__) if ex.__cause__ else str(ex))

    def visit_unpause_stage(self, stage: stages.UnpauseStage, plan: EvaluatablePlan) -> None:
        self.state_sync.unpause_snapshots(stage.promoted_snapshots, plan.end)

    def visit_virtual_layer_update_stage(
        self, stage: stages.VirtualLayerUpdateStage, plan: EvaluatablePlan
    ) -> None:
        environment = plan.environment

        self.console.start_promotion_progress(
            list(stage.promoted_snapshots) + list(stage.demoted_snapshots),
            environment.naming_info,
            self.default_catalog,
        )

        completed = False
        try:
            self._promote_snapshots(
                plan,
                [stage.all_snapshots[s.snapshot_id] for s in stage.promoted_snapshots],
                environment.naming_info,
                deployability_index=stage.deployability_index,
                on_complete=lambda s: self.console.update_promotion_progress(s, True),
                snapshots=stage.all_snapshots,
            )
            if stage.demoted_environment_naming_info:
                self._demote_snapshots(
                    stage.demoted_snapshots,
                    stage.demoted_environment_naming_info,
                    on_complete=lambda s: self.console.update_promotion_progress(s, False),
                )

            completed = True
        finally:
            self.console.stop_promotion_progress(success=completed)

    def visit_finalize_environment_stage(
        self, stage: stages.FinalizeEnvironmentStage, plan: EvaluatablePlan
    ) -> None:
        self.state_sync.finalize(plan.environment)

    def _promote_snapshots(
        self,
        plan: EvaluatablePlan,
        target_snapshots: t.Iterable[Snapshot],
        environment_naming_info: EnvironmentNamingInfo,
        snapshots: t.Dict[SnapshotId, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex] = None,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        self.snapshot_evaluator.promote(
            target_snapshots,
            start=plan.start,
            end=plan.end,
            execution_time=plan.execution_time or now(),
            snapshots=snapshots,
            table_mapping=to_view_mapping(
                snapshots.values(),
                environment_naming_info,
                default_catalog=self.default_catalog,
                dialect=self.snapshot_evaluator.adapter.dialect,
            ),
            environment_naming_info=environment_naming_info,
            deployability_index=deployability_index,
            on_complete=on_complete,
        )

    def _demote_snapshots(
        self,
        target_snapshots: t.Iterable[SnapshotTableInfo],
        environment_naming_info: EnvironmentNamingInfo,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        self.snapshot_evaluator.demote(
            target_snapshots, environment_naming_info, on_complete=on_complete
        )

    def _restatement_intervals_across_all_environments(
        self,
        prod_restatements: t.Dict[str, Interval],
        disable_restatement_models: t.Set[str],
        loaded_snapshots: t.Dict[SnapshotId, Snapshot],
    ) -> t.Set[t.Tuple[SnapshotTableInfo, Interval]]:
        """
        Given a map of snapshot names + intervals to restate in prod:
         - Look up matching snapshots across all environments (match based on name - regardless of version)
         - For each match, also match downstream snapshots while filtering out models that have restatement disabled
         - Return all matches mapped to the intervals of the prod snapshot being restated

        The goal here is to produce a list of intervals to invalidate across all environments so that a cadence
        run in those environments causes the intervals to be repopulated
        """
        if not prod_restatements:
            return set()

        snapshots_to_restate: t.Dict[SnapshotId, t.Tuple[SnapshotTableInfo, Interval]] = {}

        for env_summary in self.state_sync.get_environments_summary():
            # Fetch the full environment object one at a time to avoid loading all environments into memory at once
            env = self.state_sync.get_environment(env_summary.name)
            if not env:
                logger.warning("Environment %s not found", env_summary.name)
                continue

            keyed_snapshots = {s.name: s.table_info for s in env.snapshots}

            # We dont just restate matching snapshots, we also have to restate anything downstream of them
            # so that if A gets restated in prod and dev has A <- B <- C, B and C get restated in dev
            env_dag = DAG({s.name: {p.name for p in s.parents} for s in env.snapshots})

            for restatement, intervals in prod_restatements.items():
                if restatement not in keyed_snapshots:
                    continue
                affected_snapshot_names = [
                    x
                    for x in ([restatement] + env_dag.downstream(restatement))
                    if x not in disable_restatement_models
                ]
                snapshots_to_restate.update(
                    {
                        keyed_snapshots[a].snapshot_id: (keyed_snapshots[a], intervals)
                        for a in affected_snapshot_names
                    }
                )

        # for any affected full_history_restatement_only snapshots, we need to widen the intervals being restated to
        # include the whole time range for that snapshot. This requires a call to state to load the full snapshot record,
        # so we only do it if necessary
        full_history_restatement_snapshot_ids = [
            # FIXME: full_history_restatement_only is just one indicator that the snapshot can only be fully refreshed, the other one is Model.depends_on_self
            # however, to figure out depends_on_self, we have to render all the model queries which, alongside having to fetch full snapshots from state,
            # is problematic in secure environments that are deliberately isolated from arbitrary user code (since rendering a query may require user macros to be present)
            # So for now, these are not considered
            s_id
            for s_id, s in snapshots_to_restate.items()
            if s[0].full_history_restatement_only
        ]
        if full_history_restatement_snapshot_ids:
            # only load full snapshot records that we havent already loaded
            additional_snapshots = self.state_sync.get_snapshots(
                [
                    s.snapshot_id
                    for s in full_history_restatement_snapshot_ids
                    if s.snapshot_id not in loaded_snapshots
                ]
            )

            all_snapshots = loaded_snapshots | additional_snapshots

            for full_snapshot_id in full_history_restatement_snapshot_ids:
                full_snapshot = all_snapshots[full_snapshot_id]
                _, original_intervals = snapshots_to_restate[full_snapshot_id]
                original_start, original_end = original_intervals

                # get_removal_interval() widens intervals if necessary
                new_intervals = full_snapshot.get_removal_interval(
                    start=original_start, end=original_end
                )
                snapshots_to_restate[full_snapshot_id] = (full_snapshot.table_info, new_intervals)

        return set(snapshots_to_restate.values())

    def _update_intervals_for_new_snapshots(self, snapshots: t.Collection[Snapshot]) -> None:
        snapshots_intervals: t.List[SnapshotIntervals] = []
        for snapshot in snapshots:
            if snapshot.is_forward_only:
                snapshots_intervals.append(
                    SnapshotIntervals(
                        name=snapshot.name,
                        identifier=snapshot.identifier,
                        version=snapshot.version,
                        dev_version=snapshot.dev_version,
                        dev_intervals=snapshot.dev_intervals,
                    )
                )

        if snapshots_intervals:
            self.state_sync.add_snapshots_intervals(snapshots_intervals)
