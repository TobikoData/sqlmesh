import abc
import typing as t

from dataclasses import dataclass
from rich.console import Console as RichConsole
from rich.tree import Tree
from sqlglot.dialects.dialect import DialectType
from sqlmesh.core import constants as c
from sqlmesh.core.console import Console, TerminalConsole, get_console
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.plan.definition import EvaluatablePlan, SnapshotIntervals
from sqlmesh.core.plan.evaluator import (
    PlanEvaluator,
    get_audit_only_snapshots,
    get_snapshots_to_create,
)
from sqlmesh.core.state_sync import StateReader
from sqlmesh.core.scheduler import merged_missing_intervals, SnapshotToIntervals
from sqlmesh.core.snapshot.definition import (
    DeployabilityIndex,
    Snapshot,
    SnapshotInfoMixin,
    SnapshotTableInfo,
    Interval,
)
from sqlmesh.utils import Verbosity, rich as srich
from sqlmesh.utils.date import to_ts
from sqlmesh.utils.errors import SQLMeshError


class PlanExplainer(PlanEvaluator):
    def __init__(
        self,
        state_reader: StateReader,
        default_catalog: t.Optional[str],
        console: t.Optional[Console] = None,
    ):
        self.state_reader = state_reader
        self.default_catalog = default_catalog
        self.console = console or get_console()

    def evaluate(
        self, plan: EvaluatablePlan, circuit_breaker: t.Optional[t.Callable[[], bool]] = None
    ) -> None:
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

        steps: t.List[PlanStep] = []

        before_all = [
            statement
            for environment_statements in plan.environment_statements or []
            for statement in environment_statements.before_all
        ]
        if before_all:
            steps.append(BeforeAllStep(statements=before_all))

        snapshots_to_create = [
            s
            for s in get_snapshots_to_create(plan, snapshots)
            if s in snapshots_to_intervals and s.is_model and not s.is_symbolic
        ]
        if snapshots_to_create:
            steps.append(
                PhysicalLayerUpdateStep(
                    snapshots=snapshots_to_create,
                    deployability_index=deployability_index_for_creation,
                )
            )

        audit_only_snapshots = get_audit_only_snapshots(new_snapshots, self.state_reader)
        if audit_only_snapshots:
            steps.append(AuditOnlyRunStep(snapshots=list(audit_only_snapshots.values())))

        if plan.restatements and not plan.is_dev:
            snapshot_intervals_to_restate = {}
            for name, interval in plan.restatements.items():
                restated_snapshot = snapshots_by_name[name]
                restated_snapshot.remove_interval(interval)
                snapshot_intervals_to_restate[restated_snapshot.table_info] = interval
            steps.append(RestatementStep(snapshot_intervals=snapshot_intervals_to_restate))

        if before_promote_snapshots and not plan.empty_backfill and not plan.skip_backfill:
            missing_intervals_before_promote = {
                s: i
                for s, i in snapshots_to_intervals.items()
                if s.snapshot_id in before_promote_snapshots
            }
            if missing_intervals_before_promote:
                steps.append(
                    BackfillStep(
                        snapshot_to_intervals=missing_intervals_before_promote,
                        deployability_index=deployability_index,
                    )
                )

        steps.append(UpdateEnvironmentRecordStep())

        if snapshots_with_schema_migration:
            steps.append(MigrateSchemasStep(snapshots=snapshots_with_schema_migration))

        if after_promote_snapshots and not plan.empty_backfill and not plan.skip_backfill:
            missing_intervals_after_promote = {
                s: i
                for s, i in snapshots_to_intervals.items()
                if s.snapshot_id in after_promote_snapshots
            }
            if missing_intervals_after_promote:
                steps.append(
                    BackfillStep(
                        snapshot_to_intervals=missing_intervals_after_promote,
                        deployability_index=deployability_index,
                    )
                )

        promoted_snapshots, demoted_snapshots = self._get_promoted_demoted_snapshots(plan)
        if promoted_snapshots or demoted_snapshots:
            steps.append(
                UpdateVirtualLayerStep(
                    promoted_snapshots=promoted_snapshots,
                    demoted_snapshots=demoted_snapshots,
                    deployability_index=deployability_index,
                )
            )

        after_all = [
            statement
            for environment_statements in plan.environment_statements or []
            for statement in environment_statements.after_all
        ]
        if after_all:
            steps.append(AfterAllStep(statements=after_all))

        explainer_console = _get_explainer_console(
            self.console, plan.environment, self.default_catalog
        )
        explainer_console.explain(steps)

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


@dataclass
class BeforeAllStep:
    statements: t.List[str]


@dataclass
class AfterAllStep:
    statements: t.List[str]


@dataclass
class PhysicalLayerUpdateStep:
    snapshots: t.List[Snapshot]
    deployability_index: DeployabilityIndex


@dataclass
class AuditOnlyRunStep:
    snapshots: t.List[Snapshot]


@dataclass
class RestatementStep:
    snapshot_intervals: t.Dict[SnapshotTableInfo, Interval]


@dataclass
class BackfillStep:
    snapshot_to_intervals: SnapshotToIntervals
    deployability_index: DeployabilityIndex
    before_promote: bool = True


@dataclass
class MigrateSchemasStep:
    snapshots: t.List[Snapshot]


@dataclass
class UpdateVirtualLayerStep:
    promoted_snapshots: t.Set[SnapshotTableInfo]
    demoted_snapshots: t.Set[SnapshotTableInfo]
    deployability_index: DeployabilityIndex


@dataclass
class UpdateEnvironmentRecordStep:
    pass


PlanStep = t.Union[
    BeforeAllStep,
    AfterAllStep,
    PhysicalLayerUpdateStep,
    AuditOnlyRunStep,
    RestatementStep,
    BackfillStep,
    MigrateSchemasStep,
    UpdateVirtualLayerStep,
    UpdateEnvironmentRecordStep,
]


class ExplainerConsole(abc.ABC):
    @abc.abstractmethod
    def explain(self, steps: t.List[PlanStep]) -> None:
        pass


MAX_TREE_LENGTH = 10


class RichExplainerConsole(ExplainerConsole):
    def __init__(
        self,
        environment_naming_info: EnvironmentNamingInfo,
        dialect: DialectType,
        default_catalog: t.Optional[str],
        verbosity: Verbosity = Verbosity.DEFAULT,
        console: t.Optional[RichConsole] = None,
    ):
        self.environment_naming_info = environment_naming_info
        self.dialect = dialect
        self.default_catalog = default_catalog
        self.verbosity = verbosity
        self.console: RichConsole = console or srich.console

    def explain(self, steps: t.List[PlanStep]) -> None:
        tree = Tree("[bold]Explained plan[/bold]")
        for step in steps:
            handler_name = f"visit_{_to_snake_case(step.__class__.__name__)}"
            if not hasattr(self, handler_name):
                raise SQLMeshError(f"Unexpected step: {step.__class__.__name__}")
            handler = getattr(self, handler_name)
            result = handler(step)
            if result:
                tree.add(self._limit_tree(result))
        self.console.print(tree)

    def visit_before_all_step(self, step: BeforeAllStep) -> Tree:
        tree = Tree("[bold]Execute before all statements[/bold]")
        for statement in step.statements:
            tree.add(statement)
        return tree

    def visit_after_all_step(self, step: AfterAllStep) -> Tree:
        tree = Tree("[bold]Execute after all statements[/bold]")
        for statement in step.statements:
            tree.add(statement)
        return tree

    def visit_physical_layer_update_step(self, step: PhysicalLayerUpdateStep) -> Tree:
        tree = Tree("[bold]Validate SQL and create physical tables if they do not exist[/bold]")
        for snapshot in step.snapshots:
            is_deployable = (
                step.deployability_index.is_deployable(snapshot)
                if self.environment_naming_info.name != c.PROD
                else True
            )
            display_name = self._display_name(snapshot)
            table_name = snapshot.table_name(is_deployable)
            model_tree = Tree(f"{display_name} -> {table_name}")

            if snapshot.is_model:
                if snapshot.model.pre_statements:
                    model_tree.add("Run pre-statements")
                if snapshot.model.annotated:
                    model_tree.add("Dry run model query without inserting results")

            if snapshot.is_view:
                create_tree = Tree("Create view if it doesn't exist")
            elif snapshot.is_forward_only and snapshot.previous_versions:
                prod_table = snapshot.table_name(True)
                create_tree = Tree(
                    f"Clone {prod_table} into {table_name} and then update its schema if it doesn't exist"
                )
            else:
                create_tree = Tree("Create table if it doesn't exist")

            if not is_deployable:
                create_tree.add("[orange1]preview[/orange1]: data will NOT be reused in production")
            model_tree.add(create_tree)

            if snapshot.is_model and snapshot.model.post_statements:
                model_tree.add("Run post-statements")

            tree.add(model_tree)
        return tree

    def visit_audit_only_run_step(self, step: AuditOnlyRunStep) -> Tree:
        tree = Tree("[bold]Audit-only execution[/bold]")
        for snapshot in step.snapshots:
            display_name = self._display_name(snapshot)
            tree.add(display_name)
        return tree

    def visit_restatement_step(self, step: RestatementStep) -> Tree:
        tree = Tree("[bold]Invalidate data intervals as part of restatement[/bold]")
        for snapshot_table_info, interval in step.snapshot_intervals.items():
            display_name = self._display_name(snapshot_table_info)
            tree.add(f"{display_name} [{to_ts(interval[0])} - {to_ts(interval[1])}]")
        return tree

    def visit_backfill_step(self, step: BackfillStep) -> Tree:
        tree = Tree(
            "[bold]Backfill models by running their queries and run standalone audits[/bold]"
        )
        for snapshot, intervals in step.snapshot_to_intervals.items():
            display_name = self._display_name(snapshot)
            if snapshot.is_model:
                table_name = snapshot.table_name(step.deployability_index.is_deployable(snapshot))
                model_tree = Tree(f"{display_name} -> {table_name}")

                for signal_name, _ in snapshot.model.signals:
                    model_tree.add(f"Check '{signal_name}' signal")

                if snapshot.model.pre_statements:
                    model_tree.add("Run pre-statements")

                if snapshot.is_incremental:
                    current_intervals = (
                        snapshot.intervals
                        if step.deployability_index.is_deployable(snapshot)
                        else snapshot.dev_intervals
                    )
                    if current_intervals:
                        formatted_range = SnapshotIntervals(
                            snapshot_id=snapshot.snapshot_id, intervals=intervals
                        ).format_intervals(snapshot.node.interval_unit)
                        model_tree.add(
                            f"Incrementally insert records within the range [{formatted_range}]"
                        )
                    else:
                        # If there are no intervals, the table will be fully refreshed
                        model_tree.add("Fully refresh table")
                elif snapshot.is_view:
                    model_tree.add("Recreate view")
                else:
                    model_tree.add("Fully refresh table")

                if snapshot.model.post_statements:
                    model_tree.add("Run post-statements")

                if snapshot.model.audits:
                    for audit_name, _ in snapshot.model.audits:
                        model_tree.add(f"Run '{audit_name}' audit")

                tree.add(model_tree)
            else:
                tree.add(f"{display_name} \[standalone audit]")
        return tree

    def visit_migrate_schemas_step(self, step: MigrateSchemasStep) -> Tree:
        tree = Tree(
            "[bold]Update schemas (add, drop, alter columns) of production physical tables to reflect forward-only changes[/bold]"
        )
        for snapshot in step.snapshots:
            display_name = self._display_name(snapshot)
            table_name = snapshot.table_name(True)
            tree.add(f"{display_name} -> {table_name}")
        return tree

    def visit_update_virtual_layer_step(self, step: UpdateVirtualLayerStep) -> Tree:
        tree = Tree(
            f"[bold]Update the virtual layer for environment '{self.environment_naming_info.name}'[/bold]"
        )
        promote_tree = Tree(
            "[bold]Create or update views in the virtual layer to point at new physical tables[/bold]"
        )
        for snapshot in step.promoted_snapshots:
            display_name = self._display_name(snapshot)
            table_name = snapshot.table_name(step.deployability_index.is_representative(snapshot))
            promote_tree.add(f"{display_name} -> {table_name}")

        demote_tree = Tree(
            "[bold]Delete views in the virtual layer for models that were removed[/bold]"
        )
        for snapshot in step.demoted_snapshots:
            display_name = self._display_name(snapshot)
            demote_tree.add(display_name)

        if step.promoted_snapshots:
            tree.add(self._limit_tree(promote_tree))
        if step.demoted_snapshots:
            tree.add(self._limit_tree(demote_tree))
        return tree

    def visit_update_environment_record_step(
        self, step: UpdateEnvironmentRecordStep
    ) -> t.Optional[Tree]:
        return None

    def _display_name(self, snapshot: SnapshotInfoMixin) -> str:
        return snapshot.display_name(
            self.environment_naming_info,
            self.default_catalog if self.verbosity < Verbosity.VERY_VERBOSE else None,
            dialect=self.dialect,
        )

    def _limit_tree(self, tree: Tree) -> Tree:
        tree_length = len(tree.children)
        if tree_length <= MAX_TREE_LENGTH:
            return tree
        if self.verbosity < Verbosity.VERY_VERBOSE:
            tree.children = [
                tree.children[0],
                Tree(f".... {tree_length - 2} more ...."),
                tree.children[-1],
            ]
        return tree


def _get_explainer_console(
    console: t.Optional[Console],
    environment_naming_info: EnvironmentNamingInfo,
    default_catalog: t.Optional[str],
) -> ExplainerConsole:
    console = console or get_console()
    if not isinstance(console, TerminalConsole):
        raise SQLMeshError("Plain explaination is only supported in the terminal.")
    return RichExplainerConsole(
        environment_naming_info=environment_naming_info,
        dialect=console.dialect,
        default_catalog=default_catalog,
        verbosity=console.verbosity,
        console=console.console,
    )


def _to_snake_case(name: str) -> str:
    return "".join(
        f"_{c.lower()}" if c.isupper() and idx != 0 else c.lower() for idx, c in enumerate(name)
    )
