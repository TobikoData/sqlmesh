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
from sqlmesh.utils import CompletionStatus
from sqlmesh.core.state_sync import StateSync
from sqlmesh.core.state_sync.base import PromotionResult
from sqlmesh.utils.concurrency import NodeExecutionFailedError
from sqlmesh.utils.errors import PlanError
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

            deployability_index_for_evaluation = DeployabilityIndex.create(
                snapshots, start=plan.start
            )
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

            # Step 1: Run before_all environment statements before doing anything else
            execute_environment_statements(
                adapter=self.snapshot_evaluator.adapter,
                environment_statements=plan.environment_statements or [],
                runtime_stage=RuntimeStage.BEFORE_ALL,
                environment_naming_info=plan.environment.naming_info,
                default_catalog=self.default_catalog,
                snapshots=snapshots_by_name,
                start=plan.start,
                end=plan.end,
                execution_time=plan.execution_time,
            )

            # Step 2:Store new snapshot records in the state and create physical tables for them
            push_completion_status = self._push(plan, snapshots, deployability_index_for_creation)
            if push_completion_status.is_nothing_to_do:
                self.console.log_success(
                    "" if plan.restatements else "\nSKIP: No physical layer updates to perform"
                )

            # Step 3: Update the intervals for the new forward-only snapshots
            update_intervals_for_new_snapshots(plan.new_snapshots, self.state_sync)

            # Step 4: Run audits without evaluations for snapshots that capture audit metadata changes
            self._run_audits_for_metadata_snapshots(plan, new_snapshots)

            # Step 5: Remove intervals for snapshots that need to be restated
            self._restate(plan, snapshots_by_name)

            # Step 6: Backfill missing intervals for snapshots that can be backfilled before updating
            # the schema of production tables
            first_bf_completion_status = self._backfill(
                plan,
                snapshots_by_name,
                before_promote_snapshots,
                deployability_index_for_evaluation,
                circuit_breaker=circuit_breaker,
            )

            # Step 7: Update the target environment record in the state and migrate table schemas for forward-only
            # snapshots if deploying to production
            promotion_result = self._promote(
                plan, snapshots, before_promote_snapshots, deployability_index_for_creation
            )

            # Step 8: Backfill missing intervals for snapshots that can be backfilled only after updating
            # the schema of production tables
            second_bf_completion_status = self._backfill(
                plan,
                snapshots_by_name,
                after_promote_snapshots,
                deployability_index_for_evaluation,
                circuit_breaker=circuit_breaker,
            )

            if (
                first_bf_completion_status.is_nothing_to_do
                and second_bf_completion_status.is_nothing_to_do
            ):
                self.console.log_success("SKIP: No model batches to execute")

            # Step 9: Update environment views to point at new physical tables and finalize the environment
            # record in the state
            self._update_views(
                plan, snapshots, promotion_result, deployability_index_for_evaluation
            )

            # Step 10: Run after_all environment statements
            execute_environment_statements(
                adapter=self.snapshot_evaluator.adapter,
                environment_statements=plan.environment_statements or [],
                runtime_stage=RuntimeStage.AFTER_ALL,
                environment_naming_info=plan.environment.naming_info,
                default_catalog=self.default_catalog,
                snapshots=snapshots_by_name,
                start=plan.start,
                end=plan.end,
                execution_time=plan.execution_time,
            )

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
    ) -> CompletionStatus:
        """Backfill missing intervals for snapshots that are part of the given plan.

        Args:
            plan: The plan to source snapshots from.
            selected_snapshots: The snapshots to backfill.
        """
        if plan.empty_backfill:
            intervals_to_add = []
            for snapshot in snapshots_by_name.values():
                if not snapshot.evaluatable or not plan.is_selected_for_backfill(snapshot.name):
                    # Skip snapshots that are not evaluatable or not selected for backfill.
                    continue
                intervals = [
                    snapshot.inclusive_exclusive(plan.start, plan.end, strict=False, expand=False)
                ]
                is_deployable = deployability_index.is_deployable(snapshot)
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
            return CompletionStatus.NOTHING_TO_DO

        if not plan.requires_backfill or not selected_snapshots:
            return CompletionStatus.NOTHING_TO_DO

        scheduler = self.create_scheduler(snapshots_by_name.values())
        completion_status = scheduler.run(
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
        if completion_status.is_failure:
            raise PlanError("Plan application failed.")

        return completion_status

    def _push(
        self,
        plan: EvaluatablePlan,
        snapshots: t.Dict[SnapshotId, Snapshot],
        deployability_index: t.Optional[DeployabilityIndex] = None,
    ) -> CompletionStatus:
        """Push the snapshots to the state sync.

        As a part of plan pushing, snapshot tables are created.

        Args:
            plan: The plan to source snapshots from.
            deployability_index: Indicates which snapshots are deployable in the context of this creation.
        """
        self.state_sync.push_snapshots(plan.new_snapshots)
        analytics.collector.on_snapshots_created(
            new_snapshots=plan.new_snapshots, plan_id=plan.plan_id
        )

        promoted_snapshot_ids = (
            set(plan.environment.promoted_snapshot_ids)
            if plan.environment.promoted_snapshot_ids is not None
            else None
        )

        def _should_create(s: Snapshot) -> bool:
            if not s.is_model or s.is_symbolic:
                return False
            # Only create tables for snapshots that we're planning to promote or that were selected for backfill
            return (
                plan.is_selected_for_backfill(s.name)
                or promoted_snapshot_ids is None
                or s.snapshot_id in promoted_snapshot_ids
            )

        snapshots_to_create = [s for s in snapshots.values() if _should_create(s)]

        completion_status = None
        progress_stopped = False
        try:
            completion_status = self.snapshot_evaluator.create(
                snapshots_to_create,
                snapshots,
                allow_destructive_snapshots=plan.allow_destructive_models,
                deployability_index=deployability_index,
                on_start=lambda x: self.console.start_creation_progress(
                    x, plan.environment, self.default_catalog
                ),
                on_complete=self.console.update_creation_progress,
            )
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

        return completion_status

    def _promote(
        self,
        plan: EvaluatablePlan,
        snapshots: t.Dict[SnapshotId, Snapshot],
        no_gaps_snapshot_names: t.Optional[t.Set[str]] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
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
            environment_statements=plan.environment_statements,
        )

        if not plan.is_dev:
            try:
                self.snapshot_evaluator.migrate(
                    [s for s in snapshots.values() if s.is_paused],
                    snapshots,
                    allow_destructive_snapshots=plan.allow_destructive_models,
                    deployability_index=deployability_index,
                )
            except NodeExecutionFailedError as ex:
                raise PlanError(str(ex.__cause__) if ex.__cause__ else str(ex))

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
            promotion_result.added + promotion_result.removed,
            environment.naming_info,
            self.default_catalog,
        )

        completed = False
        try:
            self._promote_snapshots(
                plan,
                [snapshots[s.snapshot_id] for s in promotion_result.added],
                environment.naming_info,
                deployability_index=deployability_index,
                on_complete=lambda s: self.console.update_promotion_progress(s, True),
                snapshots=snapshots,
            )
            if promotion_result.removed_environment_naming_info:
                self._demote_snapshots(
                    plan,
                    promotion_result.removed,
                    promotion_result.removed_environment_naming_info,
                    on_complete=lambda s: self.console.update_promotion_progress(s, False),
                )

            self.state_sync.finalize(environment)
            completed = True
        finally:
            self.console.stop_promotion_progress(success=completed)

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
        plan: EvaluatablePlan,
        target_snapshots: t.Iterable[SnapshotTableInfo],
        environment_naming_info: EnvironmentNamingInfo,
        on_complete: t.Optional[t.Callable[[SnapshotInfoLike], None]] = None,
    ) -> None:
        self.snapshot_evaluator.demote(
            target_snapshots, environment_naming_info, on_complete=on_complete
        )

    def _restate(self, plan: EvaluatablePlan, snapshots_by_name: t.Dict[str, Snapshot]) -> None:
        if not plan.restatements or plan.is_dev:
            return

        snapshot_intervals_to_restate = {
            (snapshots_by_name[name].table_info, intervals)
            for name, intervals in plan.restatements.items()
        }

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
                loaded_snapshots={s.snapshot_id: s for s in snapshots_by_name.values()},
            )
        )

        self.state_sync.remove_intervals(
            snapshot_intervals=list(snapshot_intervals_to_restate),
            remove_shared_versions=plan.is_prod,
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

        for env in self.state_sync.get_environments():
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

    def _run_audits_for_metadata_snapshots(
        self,
        plan: EvaluatablePlan,
        new_snapshots: t.Dict[SnapshotId, Snapshot],
    ) -> None:
        # Filter out snapshots that are not categorized as metadata changes on models
        metadata_snapshots = []
        for snapshot in new_snapshots.values():
            if not snapshot.is_metadata or not snapshot.is_model or not snapshot.evaluatable:
                continue

            metadata_snapshots.append(snapshot)

        # Bulk load all the previous snapshots
        previous_snapshots = self.state_sync.get_snapshots(
            [
                s.previous_version.snapshot_id(s.name)
                for s in metadata_snapshots
                if s.previous_version
            ]
        ).values()

        # Check if any of the snapshots have modifications to the audits field by comparing the hashes
        audit_snapshots = {}
        for snapshot, previous_snapshot in zip(metadata_snapshots, previous_snapshots):
            new_audits_hash = snapshot.model.audit_metadata_hash()
            previous_audit_hash = previous_snapshot.model.audit_metadata_hash()

            if snapshot.model.audits and previous_audit_hash != new_audits_hash:
                audit_snapshots[snapshot.snapshot_id] = snapshot

        if not audit_snapshots:
            return

        # If there are any snapshots to be audited, we'll reuse the scheduler's internals to audit them
        scheduler = self.create_scheduler(audit_snapshots.values())
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


def update_intervals_for_new_snapshots(
    snapshots: t.Collection[Snapshot], state_sync: StateSync
) -> None:
    snapshots_intervals: t.List[SnapshotIntervals] = []
    for snapshot in state_sync.refresh_snapshot_intervals(snapshots):
        if snapshot.is_forward_only:
            snapshot.dev_intervals = snapshot.intervals.copy()
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
        state_sync.add_snapshots_intervals(snapshots_intervals)
