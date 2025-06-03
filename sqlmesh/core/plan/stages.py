import typing as t

from dataclasses import dataclass
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.plan.evaluator import (
    get_audit_only_snapshots,
    get_snapshots_to_create,
)
from sqlmesh.core.state_sync import StateReader
from sqlmesh.core.scheduler import merged_missing_intervals, SnapshotToIntervals
from sqlmesh.core.snapshot.definition import (
    DeployabilityIndex,
    Snapshot,
    SnapshotTableInfo,
    SnapshotId,
    Interval,
)


@dataclass
class BeforeAllStage:
    statements: t.List[str]


@dataclass
class AfterAllStage:
    statements: t.List[str]


@dataclass
class PhysicalLayerUpdateStage:
    snapshots: t.List[Snapshot]
    deployability_index: DeployabilityIndex


@dataclass
class AuditOnlyRunStage:
    snapshots: t.List[Snapshot]


@dataclass
class RestatementStage:
    snapshot_intervals: t.Dict[SnapshotTableInfo, Interval]


@dataclass
class BackfillStage:
    snapshot_to_intervals: SnapshotToIntervals
    deployability_index: DeployabilityIndex
    before_promote: bool = True


@dataclass
class MigrateSchemasStage:
    snapshots: t.List[Snapshot]


@dataclass
class VirtualLayerUpdateStage:
    promoted_snapshots: t.Set[SnapshotTableInfo]
    demoted_snapshots: t.Set[SnapshotTableInfo]
    deployability_index: DeployabilityIndex


@dataclass
class EnvironmentRecordUpdateStage:
    pass


PlanStage = t.Union[
    BeforeAllStage,
    AfterAllStage,
    PhysicalLayerUpdateStage,
    AuditOnlyRunStage,
    RestatementStage,
    BackfillStage,
    MigrateSchemasStage,
    VirtualLayerUpdateStage,
    EnvironmentRecordUpdateStage,
]


class PlanStagesBuilder:
    def __init__(
        self,
        state_reader: StateReader,
        default_catalog: t.Optional[str],
    ):
        self.state_reader = state_reader
        self.default_catalog = default_catalog

    def build(self, plan: EvaluatablePlan) -> t.List[PlanStage]:
        new_snapshots = {s.snapshot_id: s for s in plan.new_snapshots}
        stored_snapshots = self.state_reader.get_snapshots(plan.environment.snapshots)
        snapshots = {**new_snapshots, **stored_snapshots}
        snapshots_by_name = {s.name: s for s in snapshots.values()}

        all_selected_for_backfill_snapshots = {
            s.snapshot_id for s in snapshots.values() if plan.is_selected_for_backfill(s.name)
        }

        deployability_index = DeployabilityIndex.create(snapshots, start=plan.start)
        deployability_index_for_creation = deployability_index
        if plan.is_dev:
            before_promote_snapshots = all_selected_for_backfill_snapshots
            after_promote_snapshots = set()
            snapshots_with_schema_migration = []
        else:
            before_promote_snapshots = {
                s.snapshot_id
                for s in snapshots.values()
                if deployability_index.is_representative(s)
                and plan.is_selected_for_backfill(s.name)
            }
            after_promote_snapshots = all_selected_for_backfill_snapshots - before_promote_snapshots
            deployability_index = DeployabilityIndex.all_deployable()

            snapshots_with_schema_migration = [
                s
                for s in snapshots.values()
                if s.is_paused
                and s.is_materialized
                and not deployability_index_for_creation.is_representative(s)
            ]

        snapshots_to_intervals = self._missing_intervals(
            plan, snapshots_by_name, deployability_index
        )
        needs_backfill = (
            not plan.empty_backfill and not plan.skip_backfill and bool(snapshots_to_intervals)
        )
        missing_intervals_before_promote: SnapshotToIntervals = {}
        missing_intervals_after_promote: SnapshotToIntervals = {}
        if needs_backfill:
            for snapshot, intervals in snapshots_to_intervals.items():
                if snapshot.snapshot_id in before_promote_snapshots:
                    missing_intervals_before_promote[snapshot] = intervals
                elif snapshot.snapshot_id in after_promote_snapshots:
                    missing_intervals_after_promote[snapshot] = intervals

        stages: t.List[PlanStage] = []

        before_all_stage = self._get_before_all_stage(plan)
        if before_all_stage:
            stages.append(before_all_stage)

        stages.append(
            self._get_physical_layer_update_stage(
                plan, snapshots, snapshots_to_intervals, deployability_index_for_creation
            )
        )

        audit_only_snapshots = get_audit_only_snapshots(new_snapshots, self.state_reader)
        if audit_only_snapshots:
            stages.append(AuditOnlyRunStage(snapshots=list(audit_only_snapshots.values())))

        restatement_stage = self._get_restatement_stage(plan, snapshots_by_name)
        if restatement_stage:
            stages.append(restatement_stage)

        if missing_intervals_before_promote:
            stages.append(
                BackfillStage(
                    snapshot_to_intervals=missing_intervals_before_promote,
                    deployability_index=deployability_index,
                )
            )
        elif not needs_backfill:
            # Append an empty backfill stage so that explainer can show that the stage is skipped
            stages.append(
                BackfillStage(snapshot_to_intervals={}, deployability_index=deployability_index)
            )

        stages.append(EnvironmentRecordUpdateStage())

        if snapshots_with_schema_migration:
            stages.append(MigrateSchemasStage(snapshots=snapshots_with_schema_migration))

        if missing_intervals_after_promote:
            stages.append(
                BackfillStage(
                    snapshot_to_intervals=missing_intervals_after_promote,
                    deployability_index=deployability_index,
                )
            )

        virtual_layer_update_stage = self._get_virtual_layer_update_stage(plan, deployability_index)
        if virtual_layer_update_stage:
            stages.append(virtual_layer_update_stage)

        after_all_stage = self._get_after_all_stage(plan)
        if after_all_stage:
            stages.append(after_all_stage)

        return stages

    def _get_before_all_stage(self, plan: EvaluatablePlan) -> t.Optional[BeforeAllStage]:
        before_all = [
            statement
            for environment_statements in plan.environment_statements or []
            for statement in environment_statements.before_all
        ]
        return BeforeAllStage(statements=before_all) if before_all else None

    def _get_after_all_stage(self, plan: EvaluatablePlan) -> t.Optional[AfterAllStage]:
        after_all = [
            statement
            for environment_statements in plan.environment_statements or []
            for statement in environment_statements.after_all
        ]
        return AfterAllStage(statements=after_all) if after_all else None

    def _get_restatement_stage(
        self, plan: EvaluatablePlan, snapshots_by_name: t.Dict[str, Snapshot]
    ) -> t.Optional[RestatementStage]:
        snapshot_intervals_to_restate = {}
        for name, interval in plan.restatements.items():
            restated_snapshot = snapshots_by_name[name]
            restated_snapshot.remove_interval(interval)
            snapshot_intervals_to_restate[restated_snapshot.table_info] = interval
        if not snapshot_intervals_to_restate or plan.is_dev:
            return None
        return RestatementStage(snapshot_intervals=snapshot_intervals_to_restate)

    def _get_physical_layer_update_stage(
        self,
        plan: EvaluatablePlan,
        snapshots: t.Dict[SnapshotId, Snapshot],
        snapshots_to_intervals: SnapshotToIntervals,
        deployability_index: DeployabilityIndex,
    ) -> PhysicalLayerUpdateStage:
        snapshots_to_create = [
            s
            for s in get_snapshots_to_create(plan, snapshots)
            if s in snapshots_to_intervals and s.is_model and not s.is_symbolic
        ]
        return PhysicalLayerUpdateStage(
            snapshots=snapshots_to_create,
            deployability_index=deployability_index,
        )

    def _get_virtual_layer_update_stage(
        self, plan: EvaluatablePlan, deployability_index: DeployabilityIndex
    ) -> t.Optional[VirtualLayerUpdateStage]:
        promoted_snapshots, demoted_snapshots = self._get_promoted_demoted_snapshots(plan)
        if not promoted_snapshots and not demoted_snapshots:
            return None
        return VirtualLayerUpdateStage(
            promoted_snapshots=promoted_snapshots,
            demoted_snapshots=demoted_snapshots,
            deployability_index=deployability_index,
        )

    def _get_promoted_demoted_snapshots(
        self, plan: EvaluatablePlan
    ) -> t.Tuple[t.Set[SnapshotTableInfo], t.Set[SnapshotTableInfo]]:
        existing_environment = self.state_reader.get_environment(plan.environment.name)
        if existing_environment:
            snapshots_by_name = {s.name: s for s in existing_environment.snapshots}
            demoted_snapshot_names = {s.name for s in existing_environment.promoted_snapshots} - {
                s.name for s in plan.environment.promoted_snapshots
            }
            demoted_snapshots = {snapshots_by_name[name] for name in demoted_snapshot_names}
        else:
            demoted_snapshots = set()
        promoted_snapshots = set(plan.environment.promoted_snapshots)
        if existing_environment and plan.environment.can_partially_promote(existing_environment):
            promoted_snapshots -= set(existing_environment.promoted_snapshots)

        def _snapshot_filter(snapshot: SnapshotTableInfo) -> bool:
            return snapshot.is_model and not snapshot.is_symbolic

        return {s for s in promoted_snapshots if _snapshot_filter(s)}, {
            s for s in demoted_snapshots if _snapshot_filter(s)
        }

    def _missing_intervals(
        self,
        plan: EvaluatablePlan,
        snapshots_by_name: t.Dict[str, Snapshot],
        deployability_index: DeployabilityIndex,
    ) -> SnapshotToIntervals:
        return merged_missing_intervals(
            snapshots=snapshots_by_name.values(),
            start=plan.start,
            end=plan.end,
            execution_time=plan.execution_time,
            restatements={
                snapshots_by_name[name].snapshot_id: interval
                for name, interval in plan.restatements.items()
            },
            deployability_index=deployability_index,
            end_bounded=plan.end_bounded,
            interval_end_per_model=plan.interval_end_per_model,
        )


def build_plan_stages(
    plan: EvaluatablePlan,
    state_reader: StateReader,
    default_catalog: t.Optional[str],
) -> t.List[PlanStage]:
    return PlanStagesBuilder(state_reader, default_catalog).build(plan)
