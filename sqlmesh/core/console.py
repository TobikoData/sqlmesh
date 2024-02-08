from __future__ import annotations

import abc
import datetime
import typing as t
import unittest
import uuid

from hyperscript import h
from rich.console import Console as RichConsole
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.prompt import Confirm, Prompt
from rich.status import Status
from rich.syntax import Syntax
from rich.table import Table
from rich.tree import Tree

from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    SnapshotInfoLike,
    start_date,
)
from sqlmesh.core.test import ModelTest
from sqlmesh.utils import rich as srich
from sqlmesh.utils.date import to_date, yesterday_ds

if t.TYPE_CHECKING:
    import ipywidgets as widgets

    from sqlmesh.core.context_diff import ContextDiff
    from sqlmesh.core.plan import Plan, PlanBuilder
    from sqlmesh.core.table_diff import RowDiff, SchemaDiff

    LayoutWidget = t.TypeVar("LayoutWidget", bound=t.Union[widgets.VBox, widgets.HBox])


SNAPSHOT_CHANGE_CATEGORY_STR = {
    None: "Unknown",
    SnapshotChangeCategory.BREAKING: "Breaking",
    SnapshotChangeCategory.NON_BREAKING: "Non-breaking",
    SnapshotChangeCategory.FORWARD_ONLY: "Forward-only",
    SnapshotChangeCategory.INDIRECT_BREAKING: "Indirect Breaking",
    SnapshotChangeCategory.INDIRECT_NON_BREAKING: "Indirect Non-breaking",
}


class Console(abc.ABC):
    """Abstract base class for defining classes used for displaying information to the user and also interact
    with them when their input is needed."""

    @abc.abstractmethod
    def start_plan_evaluation(self, plan: Plan) -> None:
        """Indicates that a new evaluation has begun."""

    @abc.abstractmethod
    def stop_plan_evaluation(self) -> None:
        """Indicates that the evaluation has ended."""

    @abc.abstractmethod
    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        """Indicates that a new snapshot evaluation progress has begun."""

    @abc.abstractmethod
    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        """Starts the snapshot evaluation progress."""

    @abc.abstractmethod
    def update_snapshot_evaluation_progress(
        self, snapshot: Snapshot, batch_idx: int, duration_ms: t.Optional[int]
    ) -> None:
        """Updates the snapshot evaluation progress."""

    @abc.abstractmethod
    def stop_evaluation_progress(self, success: bool = True) -> None:
        """Stops the snapshot evaluation progress."""

    @abc.abstractmethod
    def start_creation_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        """Indicates that a new snapshot creation progress has begun."""

    @abc.abstractmethod
    def update_creation_progress(self, snapshot: SnapshotInfoLike) -> None:
        """Update the snapshot creation progress."""

    @abc.abstractmethod
    def stop_creation_progress(self, success: bool = True) -> None:
        """Stop the snapshot creation progress."""

    @abc.abstractmethod
    def update_cleanup_progress(self, object_name: str) -> None:
        """Update the snapshot cleanup progress."""

    @abc.abstractmethod
    def start_promotion_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        """Indicates that a new snapshot promotion progress has begun."""

    @abc.abstractmethod
    def update_promotion_progress(self, snapshot: SnapshotInfoLike, promoted: bool) -> None:
        """Update the snapshot promotion progress."""

    @abc.abstractmethod
    def stop_promotion_progress(self, success: bool = True) -> None:
        """Stop the snapshot promotion progress."""

    @abc.abstractmethod
    def start_migration_progress(self, total_tasks: int) -> None:
        """Indicates that a new migration progress has begun."""

    @abc.abstractmethod
    def update_migration_progress(self, num_tasks: int) -> None:
        """Update the migration progress."""

    @abc.abstractmethod
    def stop_migration_progress(self, success: bool = True) -> None:
        """Stop the migration progress."""

    @abc.abstractmethod
    def show_model_difference_summary(
        self,
        context_diff: ContextDiff,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        no_diff: bool = True,
        ignored_snapshot_ids: t.Optional[t.Set[SnapshotId]] = None,
    ) -> None:
        """Displays a summary of differences for the given models."""

    @abc.abstractmethod
    def plan(
        self,
        plan_builder: PlanBuilder,
        auto_apply: bool,
        default_catalog: t.Optional[str],
        no_diff: bool = False,
        no_prompts: bool = False,
    ) -> None:
        """The main plan flow.

        The console should present the user with choices on how to backfill and version the snapshots
        of a plan.

        Args:
            plan: The plan to make choices for.
            auto_apply: Whether to automatically apply the plan after all choices have been made.
            no_diff: Hide text differences for changed models.
            no_prompts: Whether to disable interactive prompts for the backfill time range. Please note that
                if this flag is set to true and there are uncategorized changes the plan creation will
                fail. Default: False
        """

    @abc.abstractmethod
    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        """Display the test result and output.

        Args:
            result: The unittest test result that contains metrics like num success, fails, ect.
            output: The generated output from the unittest.
            target_dialect: The dialect that tests were run against. Assumes all tests run against the same dialect.
        """

    @abc.abstractmethod
    def show_sql(self, sql: str) -> None:
        """Display to the user SQL."""

    @abc.abstractmethod
    def log_status_update(self, message: str) -> None:
        """Display general status update to the user."""

    @abc.abstractmethod
    def log_error(self, message: str) -> None:
        """Display error info to the user."""

    @abc.abstractmethod
    def log_success(self, message: str) -> None:
        """Display a general successful message to the user."""

    @abc.abstractmethod
    def loading_start(self, message: t.Optional[str] = None) -> uuid.UUID:
        """Starts loading and returns a unique ID that can be used to stop the loading. Optionally can display a message."""

    @abc.abstractmethod
    def loading_stop(self, id: uuid.UUID) -> None:
        """Stop loading for the given id."""

    @abc.abstractmethod
    def show_schema_diff(self, schema_diff: SchemaDiff) -> None:
        """Show table schema diff."""

    @abc.abstractmethod
    def show_row_diff(self, row_diff: RowDiff, show_sample: bool = True) -> None:
        """Show table summary diff."""


class TerminalConsole(Console):
    """A rich based implementation of the console."""

    def __init__(
        self, console: t.Optional[RichConsole] = None, verbose: bool = False, **kwargs: t.Any
    ) -> None:
        self.console: RichConsole = console or srich.console

        self.evaluation_progress_live: t.Optional[Live] = None
        self.evaluation_total_progress: t.Optional[Progress] = None
        self.evaluation_total_task: t.Optional[TaskID] = None
        self.evaluation_model_progress: t.Optional[Progress] = None
        self.evaluation_model_tasks: t.Dict[str, TaskID] = {}
        self.evaluation_model_batches: t.Dict[Snapshot, int] = {}

        # Put in temporary values that are replaced when evaluating
        self.environment_naming_info = EnvironmentNamingInfo()
        self.default_catalog: t.Optional[str] = None

        self.creation_progress: t.Optional[Progress] = None
        self.creation_task: t.Optional[TaskID] = None

        self.promotion_progress: t.Optional[Progress] = None
        self.promotion_task: t.Optional[TaskID] = None

        self.migration_progress: t.Optional[Progress] = None
        self.migration_task: t.Optional[TaskID] = None

        self.loading_status: t.Dict[uuid.UUID, Status] = {}

        self.verbose = verbose

    def _print(self, value: t.Any, **kwargs: t.Any) -> None:
        self.console.print(value, **kwargs)

    def _prompt(self, message: str, **kwargs: t.Any) -> t.Any:
        return Prompt.ask(message, console=self.console, **kwargs)

    def _confirm(self, message: str, **kwargs: t.Any) -> bool:
        return Confirm.ask(message, console=self.console, **kwargs)

    def start_plan_evaluation(self, plan: Plan) -> None:
        pass

    def stop_plan_evaluation(self) -> None:
        pass

    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        """Indicates that a new snapshot evaluation progress has begun."""
        if not self.evaluation_progress_live:
            self.evaluation_total_progress = Progress(
                TextColumn("[bold blue]Evaluating models", justify="right"),
                BarColumn(bar_width=40),
                "[progress.percentage]{task.percentage:>3.1f}%",
                "•",
                srich.BatchColumn(),
                "•",
                TimeElapsedColumn(),
                console=self.console,
            )

            self.evaluation_model_progress = Progress(
                TextColumn("{task.fields[view_name]}", justify="right"),
                SpinnerColumn(spinner_name="simpleDots"),
                console=self.console,
            )

            progress_table = Table.grid()
            progress_table.add_row(self.evaluation_total_progress)
            progress_table.add_row(self.evaluation_model_progress)

            self.evaluation_progress_live = Live(progress_table, refresh_per_second=10)
            self.evaluation_progress_live.start()

            self.evaluation_total_task = self.evaluation_total_progress.add_task(
                "Evaluating models...", total=sum(batches.values())
            )

            self.evaluation_model_batches = batches
            self.environment_naming_info = environment_naming_info
            self.default_catalog = default_catalog

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        if self.evaluation_model_progress and snapshot.name not in self.evaluation_model_tasks:
            display_name = snapshot.display_name(self.environment_naming_info, self.default_catalog)
            self.evaluation_model_tasks[snapshot.name] = self.evaluation_model_progress.add_task(
                f"Evaluating {display_name}...",
                view_name=display_name,
                total=self.evaluation_model_batches[snapshot],
            )

    def update_snapshot_evaluation_progress(
        self, snapshot: Snapshot, batch_idx: int, duration_ms: t.Optional[int]
    ) -> None:
        """Update the snapshot evaluation progress."""
        if (
            self.evaluation_total_progress
            and self.evaluation_model_progress
            and self.evaluation_progress_live
        ):
            total_batches = self.evaluation_model_batches[snapshot]

            if duration_ms:
                self.evaluation_progress_live.console.print(
                    f"[{batch_idx + 1}/{total_batches}] {snapshot.display_name(self.environment_naming_info, self.default_catalog)} [green]evaluated[/green] in {(duration_ms / 1000.0):.2f}s"
                )

            self.evaluation_total_progress.update(
                self.evaluation_total_task or TaskID(0), refresh=True, advance=1
            )

            model_task_id = self.evaluation_model_tasks[snapshot.name]
            self.evaluation_model_progress.update(model_task_id, refresh=True, advance=1)
            if self.evaluation_model_progress._tasks[model_task_id].completed >= total_batches:
                self.evaluation_model_progress.remove_task(model_task_id)

    def stop_evaluation_progress(self, success: bool = True) -> None:
        """Stop the snapshot evaluation progress."""
        if self.evaluation_progress_live:
            self.evaluation_progress_live.stop()
            if success:
                self.log_success("All model batches have been executed successfully")

        self.evaluation_progress_live = None
        self.evaluation_total_progress = None
        self.evaluation_total_task = None
        self.evaluation_model_progress = None
        self.evaluation_model_tasks = {}
        self.evaluation_model_batches = {}
        self.environment_naming_info = EnvironmentNamingInfo()
        self.default_catalog = None

    def start_creation_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        """Indicates that a new creation progress has begun."""
        if self.creation_progress is None:
            self.creation_progress = Progress(
                TextColumn("[bold blue]Creating physical tables", justify="right"),
                BarColumn(bar_width=40),
                "[progress.percentage]{task.percentage:>3.1f}%",
                "•",
                srich.BatchColumn(),
                "•",
                TimeElapsedColumn(),
                console=self.console,
            )

            self.creation_progress.start()
            self.creation_task = self.creation_progress.add_task(
                "Creating physical tables...",
                total=total_tasks,
            )

            self.environment_naming_info = environment_naming_info
            self.default_catalog = default_catalog

    def update_creation_progress(self, snapshot: SnapshotInfoLike) -> None:
        """Update the snapshot creation progress."""
        if self.creation_progress is not None and self.creation_task is not None:
            if self.verbose:
                self.creation_progress.live.console.print(
                    f"{snapshot.display_name(self.environment_naming_info, self.default_catalog)} [green]created[/green]"
                )
            self.creation_progress.update(self.creation_task, refresh=True, advance=1)

    def stop_creation_progress(self, success: bool = True) -> None:
        """Stop the snapshot creation progress."""
        self.creation_task = None
        if self.creation_progress is not None:
            self.creation_progress.stop()
            self.creation_progress = None
            if success:
                self.log_success("All model versions have been created successfully")

        self.environment_naming_info = EnvironmentNamingInfo()
        self.default_catalog = None

    def update_cleanup_progress(self, object_name: str) -> None:
        """Update the snapshot cleanup progress."""
        self._print(f"Deleted object {object_name}")

    def start_promotion_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        """Indicates that a new snapshot promotion progress has begun."""
        if self.promotion_progress is None:
            self.promotion_progress = Progress(
                TextColumn(
                    f"[bold blue]Virtually Updating '{environment_naming_info.name}'",
                    justify="right",
                ),
                BarColumn(bar_width=40),
                "[progress.percentage]{task.percentage:>3.1f}%",
                "•",
                TimeElapsedColumn(),
                console=self.console,
            )

            self.promotion_progress.start()
            self.promotion_task = self.promotion_progress.add_task(
                f"Virtually Updating {environment_naming_info.name}...",
                total=total_tasks,
            )

            self.environment_naming_info = environment_naming_info
            self.default_catalog = default_catalog

    def update_promotion_progress(self, snapshot: SnapshotInfoLike, promoted: bool) -> None:
        """Update the snapshot promotion progress."""
        if self.promotion_progress is not None and self.promotion_task is not None:
            if self.verbose:
                action_str = "[green]promoted[/green]" if promoted else "[yellow]demoted[/yellow]"
                self.promotion_progress.live.console.print(
                    f"{snapshot.display_name(self.environment_naming_info, self.default_catalog)} {action_str}"
                )
            self.promotion_progress.update(self.promotion_task, refresh=True, advance=1)

    def stop_promotion_progress(self, success: bool = True) -> None:
        """Stop the snapshot promotion progress."""
        self.promotion_task = None
        if self.promotion_progress is not None:
            self.promotion_progress.stop()
            self.promotion_progress = None
            if success:
                self.log_success("The target environment has been updated successfully")

        self.environment_naming_info = EnvironmentNamingInfo()
        self.default_catalog = None

    def start_migration_progress(self, total_tasks: int) -> None:
        """Indicates that a new migration progress has begun."""
        if self.migration_progress is None:
            self.migration_progress = Progress(
                TextColumn(f"[bold blue]Migrating snapshots", justify="right"),
                BarColumn(bar_width=40),
                "[progress.percentage]{task.percentage:>3.1f}%",
                "•",
                srich.BatchColumn(),
                "•",
                TimeElapsedColumn(),
                console=self.console,
            )

            self.migration_progress.start()
            self.migration_task = self.migration_progress.add_task(
                f"Migrating snapshots...",
                total=total_tasks,
            )

    def update_migration_progress(self, num_tasks: int) -> None:
        """Update the migration progress."""
        if self.migration_progress is not None and self.migration_task is not None:
            self.migration_progress.update(self.migration_task, refresh=True, advance=num_tasks)

    def stop_migration_progress(self, success: bool = True) -> None:
        """Stop the migration progress."""
        self.migration_task = None
        if self.migration_progress is not None:
            self.migration_progress.stop()
            self.migration_progress = None
            if success:
                self.log_success("The migration has been completed successfully")

    def show_model_difference_summary(
        self,
        context_diff: ContextDiff,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        no_diff: bool = True,
        ignored_snapshot_ids: t.Optional[t.Set[SnapshotId]] = None,
    ) -> None:
        """Shows a summary of the differences.

        Args:
            context_diff: The context diff to use to print the summary
            environment_naming_info: The environment naming info to reference when printing model names
            default_catalog: The default catalog to reference when deciding to remove catalog from display names
            no_diff: Hide the actual SQL differences.
            ignored_snapshot_ids: A set of snapshot ids that are ignored
        """
        ignored_snapshot_ids = ignored_snapshot_ids or set()
        if context_diff.is_new_environment:
            self._print(
                Tree(
                    f"[bold]New environment `{context_diff.environment}` will be created from `{context_diff.create_from}`"
                )
            )
            if not context_diff.has_snapshot_changes:
                return

        if not context_diff.has_changes:
            self._print(Tree(f"[bold]No differences when compared to `{context_diff.environment}`"))
            return

        self._print(Tree(f"[bold]Summary of differences against `{context_diff.environment}`:"))
        self._show_summary_tree_for(
            context_diff,
            "Models",
            lambda x: x.is_model,
            environment_naming_info,
            default_catalog,
            no_diff=no_diff,
            ignored_snapshot_ids=ignored_snapshot_ids,
        )
        self._show_summary_tree_for(
            context_diff,
            "Standalone Audits",
            lambda x: x.is_audit,
            environment_naming_info,
            default_catalog,
            no_diff=no_diff,
            ignored_snapshot_ids=ignored_snapshot_ids,
        )

    def plan(
        self,
        plan_builder: PlanBuilder,
        auto_apply: bool,
        default_catalog: t.Optional[str],
        no_diff: bool = False,
        no_prompts: bool = False,
    ) -> None:
        """The main plan flow.

        The console should present the user with choices on how to backfill and version the snapshots
        of a plan.

        Args:
            plan: The plan to make choices for.
            auto_apply: Whether to automatically apply the plan after all choices have been made.
            default_catalog: The default catalog to reference when deciding to remove catalog from display names
            no_diff: Hide text differences for changed models.
            no_prompts: Whether to disable interactive prompts for the backfill time range. Please note that
                if this flag is set to true and there are uncategorized changes the plan creation will
                fail. Default: False
        """
        self._prompt_categorize(
            plan_builder,
            auto_apply,
            no_diff=no_diff,
            no_prompts=no_prompts,
            default_catalog=default_catalog,
        )

        if not no_prompts:
            self._show_options_after_categorization(
                plan_builder, auto_apply, default_catalog=default_catalog
            )

        if auto_apply:
            plan_builder.apply()

    def _get_ignored_tree(
        self,
        ignored_snapshot_ids: t.Set[SnapshotId],
        snapshots: t.Dict[SnapshotId, Snapshot],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> Tree:
        ignored = Tree(f"[bold][ignored]Ignored Models (Expected Plan Start):")
        for s_id in ignored_snapshot_ids:
            snapshot = snapshots[s_id]
            ignored.add(
                f"[ignored]{snapshot.display_name(environment_naming_info, default_catalog)} ({snapshot.get_latest(start_date(snapshot, snapshots.values()))})"
            )
        return ignored

    def _show_summary_tree_for(
        self,
        context_diff: ContextDiff,
        header: str,
        snapshot_selector: t.Callable[[SnapshotInfoLike], bool],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        no_diff: bool = True,
        ignored_snapshot_ids: t.Optional[t.Set[SnapshotId]] = None,
    ) -> None:
        ignored_snapshot_ids = ignored_snapshot_ids or set()
        selected_snapshots = {
            s_id: snapshot
            for s_id, snapshot in context_diff.snapshots.items()
            if snapshot_selector(snapshot)
        }
        selected_ignored_snapshot_ids = {
            s_id for s_id in selected_snapshots if s_id in ignored_snapshot_ids
        }
        added_snapshot_ids = {
            s_id for s_id in context_diff.added if snapshot_selector(context_diff.snapshots[s_id])
        } - selected_ignored_snapshot_ids
        removed_snapshot_ids = {
            s_id
            for s_id, snapshot in context_diff.removed_snapshots.items()
            if snapshot_selector(snapshot)
        } - selected_ignored_snapshot_ids
        modified_snapshot_ids = {
            current_snapshot.snapshot_id
            for _, (current_snapshot, _) in context_diff.modified_snapshots.items()
            if snapshot_selector(current_snapshot)
        } - selected_ignored_snapshot_ids

        tree_sets = (
            added_snapshot_ids,
            removed_snapshot_ids,
            modified_snapshot_ids,
            selected_ignored_snapshot_ids,
        )
        if all(not s_ids for s_ids in tree_sets):
            return

        tree = Tree(f"[bold]{header}:")
        if added_snapshot_ids:
            added_tree = Tree(f"[bold][added]Added:")
            for s_id in added_snapshot_ids:
                snapshot = context_diff.snapshots[s_id]
                added_tree.add(
                    f"[added]{snapshot.display_name(environment_naming_info, default_catalog)}"
                )
            tree.add(added_tree)
        if removed_snapshot_ids:
            removed_tree = Tree(f"[bold][removed]Removed:")
            for s_id in removed_snapshot_ids:
                snapshot_table_info = context_diff.removed_snapshots[s_id]
                removed_tree.add(
                    f"[removed]{snapshot_table_info.display_name(environment_naming_info, default_catalog)}"
                )
            tree.add(removed_tree)
        if modified_snapshot_ids:
            direct = Tree(f"[bold][direct]Directly Modified:")
            indirect = Tree(f"[bold][indirect]Indirectly Modified:")
            metadata = Tree(f"[bold][metadata]Metadata Updated:")
            for s_id in modified_snapshot_ids:
                name = s_id.name
                display_name = context_diff.snapshots[s_id].display_name(
                    environment_naming_info, default_catalog
                )
                if context_diff.directly_modified(name):
                    direct.add(
                        f"[direct]{display_name}"
                        if no_diff
                        else Syntax(f"{display_name}\n{context_diff.text_diff(name)}", "sql")
                    )
                elif context_diff.indirectly_modified(name):
                    indirect.add(f"[indirect]{display_name}")
                elif context_diff.metadata_updated(name):
                    metadata.add(
                        f"[metadata]{display_name}"
                        if no_diff
                        else Syntax(f"{display_name}", "sql", word_wrap=True)
                    )
            if direct.children:
                tree.add(direct)
            if indirect.children:
                tree.add(indirect)
            if metadata.children:
                tree.add(metadata)
        if selected_ignored_snapshot_ids:
            tree.add(
                self._get_ignored_tree(
                    selected_ignored_snapshot_ids,
                    selected_snapshots,
                    environment_naming_info,
                    default_catalog,
                )
            )
        self._print(tree)

    def _show_options_after_categorization(
        self, plan_builder: PlanBuilder, auto_apply: bool, default_catalog: t.Optional[str]
    ) -> None:
        plan = plan_builder.build()
        if plan.forward_only and plan.new_snapshots:
            self._prompt_effective_from(plan_builder, auto_apply, default_catalog)

        if plan.requires_backfill:
            self._show_missing_dates(plan_builder.build(), default_catalog)
            self._prompt_backfill(plan_builder, auto_apply, default_catalog)
        elif plan.has_changes and not auto_apply:
            self._prompt_promote(plan_builder)
        elif plan.has_unmodified_unpromoted and not auto_apply:
            self.log_status_update("\n[bold]Virtually updating unmodified models\n")
            self._prompt_promote(plan_builder)

    def _prompt_categorize(
        self,
        plan_builder: PlanBuilder,
        auto_apply: bool,
        no_diff: bool,
        no_prompts: bool,
        default_catalog: t.Optional[str],
    ) -> None:
        """Get the user's change category for the directly modified models."""
        plan = plan_builder.build()

        self.show_model_difference_summary(
            plan.context_diff,
            plan.environment_naming_info,
            default_catalog=default_catalog,
            ignored_snapshot_ids=plan.ignored,
        )

        if not no_diff:
            self._show_categorized_snapshots(plan, default_catalog)

        for snapshot in plan.uncategorized:
            if not no_diff:
                self.show_sql(plan.context_diff.text_diff(snapshot.name))
            tree = Tree(
                f"[bold][direct]Directly Modified: {snapshot.display_name(plan.environment_naming_info, default_catalog)}"
            )
            indirect_tree = None

            for child_sid in sorted(plan.indirectly_modified.get(snapshot.snapshot_id, set())):
                child_snapshot = plan.context_diff.snapshots[child_sid]
                if not indirect_tree:
                    indirect_tree = Tree(f"[indirect]Indirectly Modified Children:")
                    tree.add(indirect_tree)
                indirect_tree.add(
                    f"[indirect]{child_snapshot.display_name(plan.environment_naming_info, default_catalog)}"
                )
            self._print(tree)
            if not no_prompts:
                self._get_snapshot_change_category(
                    snapshot, plan_builder, auto_apply, default_catalog
                )

    def _show_categorized_snapshots(self, plan: Plan, default_catalog: t.Optional[str]) -> None:
        context_diff = plan.context_diff

        for snapshot in plan.categorized:
            if not context_diff.directly_modified(snapshot.name):
                continue

            category_str = SNAPSHOT_CHANGE_CATEGORY_STR[snapshot.change_category]
            tree = Tree(
                f"[bold][direct]Directly Modified: {snapshot.display_name(plan.environment_naming_info, default_catalog)} ({category_str})"
            )
            indirect_tree = None
            for child_sid in sorted(plan.indirectly_modified.get(snapshot.snapshot_id, set())):
                child_snapshot = context_diff.snapshots[child_sid]
                if not indirect_tree:
                    indirect_tree = Tree(f"[indirect]Indirectly Modified Children:")
                    tree.add(indirect_tree)
                child_category_str = SNAPSHOT_CHANGE_CATEGORY_STR[child_snapshot.change_category]
                indirect_tree.add(
                    f"[indirect]{child_snapshot.display_name(plan.environment_naming_info, default_catalog)} ({child_category_str})"
                )
            self._print(Syntax(context_diff.text_diff(snapshot.name), "sql", word_wrap=True))
            self._print(tree)

    def _show_missing_dates(self, plan: Plan, default_catalog: t.Optional[str]) -> None:
        """Displays the models with missing dates."""
        missing_intervals = plan.missing_intervals
        if not missing_intervals:
            return
        backfill = Tree("[bold]Models needing backfill (missing dates):")
        for missing in missing_intervals:
            snapshot = plan.context_diff.snapshots[missing.snapshot_id]
            if not snapshot.is_model:
                continue

            preview_modifier = ""
            if not plan.deployability_index.is_deployable(snapshot):
                preview_modifier = " ([orange1]preview[/orange1])"

            backfill.add(
                f"{snapshot.display_name(plan.environment_naming_info, default_catalog)}: {missing.format_intervals(snapshot.node.interval_unit)}{preview_modifier}"
            )
        self._print(backfill)

    def _prompt_effective_from(
        self, plan_builder: PlanBuilder, auto_apply: bool, default_catalog: t.Optional[str]
    ) -> None:
        if not plan_builder.build().effective_from:
            effective_from = self._prompt(
                "Enter the effective date (eg. '1 year', '2020-01-01') to apply forward-only changes retroactively or blank to only apply them going forward once changes are deployed to prod"
            )
            if effective_from:
                plan_builder.set_effective_from(effective_from)

    def _prompt_backfill(
        self, plan_builder: PlanBuilder, auto_apply: bool, default_catalog: t.Optional[str]
    ) -> None:
        plan = plan_builder.build()
        is_forward_only_dev = plan.is_dev and plan.forward_only
        backfill_or_preview = "preview" if is_forward_only_dev else "backfill"

        if plan_builder.is_start_and_end_allowed:
            if not plan_builder.override_start:
                if is_forward_only_dev:
                    if plan.effective_from:
                        blank_meaning = (
                            f"to preview starting from the effective date ('{plan.effective_from}')"
                        )
                        default_start = plan.effective_from
                    else:
                        blank_meaning = "to preview starting from yesterday"
                        default_start = yesterday_ds()
                else:
                    if plan.provided_start:
                        blank_meaning = f"to backfill starting from '{plan.provided_start}'"
                    else:
                        blank_meaning = "to backfill from the beginning of history"
                    default_start = None

                start = self._prompt(
                    f"Enter the {backfill_or_preview} start date (eg. '1 year', '2020-01-01') or blank {blank_meaning}",
                )
                if start:
                    plan_builder.set_start(start)
                elif default_start:
                    plan_builder.set_start(default_start)

            if not plan_builder.override_end:
                end = self._prompt(
                    f"Enter the {backfill_or_preview} end date (eg. '1 month ago', '2020-01-01') or blank to {backfill_or_preview} up until now",
                )
                if end:
                    plan_builder.set_end(end)

            plan = plan_builder.build()

        if plan.ignored:
            self._print(
                self._get_ignored_tree(
                    plan.ignored,
                    plan.context_diff.snapshots,
                    plan.environment_naming_info,
                    default_catalog,
                )
            )
        if not auto_apply and self._confirm(f"Apply - {backfill_or_preview.capitalize()} Tables"):
            plan_builder.apply()

    def _prompt_promote(self, plan_builder: PlanBuilder) -> None:
        if self._confirm(
            f"Apply - Virtual Update",
        ):
            plan_builder.apply()

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        divider_length = 70
        if result.wasSuccessful():
            self._print("=" * divider_length)
            self._print(
                f"Successfully Ran {str(result.testsRun)} tests against {target_dialect}",
                style="green",
            )
            self._print("-" * divider_length)
        else:
            self._print("-" * divider_length)
            self._print("Test Failure Summary")
            self._print("=" * divider_length)
            self._print(
                f"Num Successful Tests: {result.testsRun - len(result.failures) - len(result.errors)}"
            )
            for test, _ in result.failures + result.errors:
                if isinstance(test, ModelTest):
                    self._print(f"Failure Test: {test.model.name} {test.test_name}")
            self._print("=" * divider_length)
            self._print(output)

    def show_sql(self, sql: str) -> None:
        self._print(Syntax(sql, "sql", word_wrap=True), crop=False)

    def log_status_update(self, message: str) -> None:
        self._print(message)

    def log_error(self, message: str) -> None:
        self._print(f"[red]{message}[/red]")

    def log_success(self, message: str) -> None:
        self._print(f"\n[green]{message}[/green]\n")

    def loading_start(self, message: t.Optional[str] = None) -> uuid.UUID:
        id = uuid.uuid4()
        self.loading_status[id] = Status(message or "", console=self.console, spinner="line")
        self.loading_status[id].start()
        return id

    def loading_stop(self, id: uuid.UUID) -> None:
        self.loading_status[id].stop()
        del self.loading_status[id]

    def show_schema_diff(self, schema_diff: SchemaDiff) -> None:
        source_name = schema_diff.source
        if schema_diff.source_alias:
            source_name = schema_diff.source_alias.upper()
        target_name = schema_diff.target
        if schema_diff.target_alias:
            target_name = schema_diff.target_alias.upper()

        first_line = f"\n[b]Schema Diff Between '[yellow]{source_name}[/yellow]' and '[green]{target_name}[/green]'"
        if schema_diff.model_name:
            first_line = (
                first_line + f" environments for model '[blue]{schema_diff.model_name}[/blue]'"
            )

        tree = Tree(first_line + ":")

        if any([schema_diff.added, schema_diff.removed, schema_diff.modified]):
            if schema_diff.added:
                added = Tree("[green]Added Columns:")
                for c, t in schema_diff.added:
                    added.add(f"[green]{c} ({t})")
                tree.add(added)

            if schema_diff.removed:
                removed = Tree("[red]Removed Columns:")
                for c, t in schema_diff.removed:
                    removed.add(f"[red]{c} ({t})")
                tree.add(removed)

            if schema_diff.modified:
                modified = Tree("[magenta]Modified Columns:")
                for c, (ft, tt) in schema_diff.modified.items():
                    modified.add(f"[magenta]{c} ({ft} -> {tt})")
                tree.add(modified)
        else:
            tree.add("[b]Schemas match")

        self.console.print(tree)

    def show_row_diff(self, row_diff: RowDiff, show_sample: bool = True) -> None:
        source_name = row_diff.source
        if row_diff.source_alias:
            source_name = row_diff.source_alias.upper()
        target_name = row_diff.target
        if row_diff.target_alias:
            target_name = row_diff.target_alias.upper()

        tree = Tree("[b]Row Counts:[/b]")
        tree.add(f" [b][blue]COMMON[/blue]:[/b] {row_diff.join_count} rows")
        tree.add(f" [b][yellow]{source_name} ONLY[/yellow]:[/b] {row_diff.s_only_count} rows")
        tree.add(f" [b][green]{target_name} ONLY[/green]:[/b] {row_diff.t_only_count} rows")
        self.console.print("\n", tree)

        self.console.print("\n[b][blue]COMMON ROWS[/blue] column comparison stats:[/b]")
        if row_diff.column_stats.shape[0] > 0:
            self.console.print(row_diff.column_stats.to_string(index=True), end="\n\n")
        else:
            self.console.print("  No columns with same name and data type in both tables")

        if show_sample:
            self.console.print("\n[b][blue]COMMON ROWS[/blue] sample data differences:[/b]")
            if row_diff.joined_sample.shape[0] > 0:
                self.console.print(row_diff.joined_sample.to_string(index=False), end="\n\n")
            else:
                self.console.print("  All joined rows match")

            if row_diff.s_sample.shape[0] > 0:
                self.console.print(f"\n[b][yellow]{source_name} ONLY[/yellow] sample rows:[/b]")
                self.console.print(row_diff.s_sample.to_string(index=False), end="\n\n")

            if row_diff.t_sample.shape[0] > 0:
                self.console.print(f"\n[b][green]{target_name} ONLY[/green] sample rows:[/b]")
                self.console.print(row_diff.t_sample.to_string(index=False), end="\n\n")

    def _get_snapshot_change_category(
        self,
        snapshot: Snapshot,
        plan_builder: PlanBuilder,
        auto_apply: bool,
        default_catalog: t.Optional[str],
    ) -> None:
        choices = self._snapshot_change_choices(
            snapshot, plan_builder.environment_naming_info, default_catalog
        )
        response = self._prompt(
            "\n".join([f"[{i+1}] {choice}" for i, choice in enumerate(choices.values())]),
            show_choices=False,
            choices=[f"{i+1}" for i in range(len(choices))],
        )
        choice = list(choices)[int(response) - 1]
        plan_builder.set_choice(snapshot, choice)

    def _snapshot_change_choices(
        self,
        snapshot: Snapshot,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        use_rich_formatting: bool = True,
    ) -> t.Dict[SnapshotChangeCategory, str]:
        direct = snapshot.display_name(environment_naming_info, default_catalog)
        if use_rich_formatting:
            direct = f"[direct]{direct}[/direct]"
        indirect = "indirectly modified children"
        if use_rich_formatting:
            indirect = f"[indirect]{indirect}[/indirect]"
        if snapshot.is_view:
            choices = {
                SnapshotChangeCategory.BREAKING: f"Update {direct} and backfill {indirect}",
                SnapshotChangeCategory.NON_BREAKING: f"Update {direct} but don't backfill {indirect}",
            }
        elif snapshot.is_symbolic:
            choices = {
                SnapshotChangeCategory.BREAKING: f"Backfill {indirect}",
                SnapshotChangeCategory.NON_BREAKING: f"Don't backfill {indirect}",
            }
        else:
            choices = {
                SnapshotChangeCategory.BREAKING: f"Backfill {direct} and {indirect}",
                SnapshotChangeCategory.NON_BREAKING: f"Backfill {direct} but not {indirect}",
            }
        labeled_choices = {
            k: f"[{SNAPSHOT_CHANGE_CATEGORY_STR[k]}] {v}" for k, v in choices.items()
        }
        return labeled_choices


def add_to_layout_widget(target_widget: LayoutWidget, *widgets: widgets.Widget) -> LayoutWidget:
    """Helper function to add a widget to a layout widget.

    Args:
        target_widget: The layout widget to add the other widget(s) to.
        *widgets: The widgets to add to the layout widget.

    Returns:
        The layout widget with the children added.
    """
    target_widget.children += tuple(widgets)
    return target_widget


class NotebookMagicConsole(TerminalConsole):
    """
    Console to be used when using the magic notebook interface (`%<command>`).
    Generally reuses the Terminal console when possible by either directly outputing what it provides
    or capturing it and converting it into a widget.
    """

    def __init__(
        self,
        display: t.Optional[t.Callable] = None,
        console: t.Optional[RichConsole] = None,
        **kwargs: t.Any,
    ) -> None:
        import ipywidgets as widgets
        from IPython.display import display as ipython_display

        super().__init__(console, **kwargs)
        self.display = display or get_ipython().user_ns.get("display", ipython_display)  # type: ignore
        self.missing_dates_output = widgets.Output()
        self.dynamic_options_after_categorization_output = widgets.VBox()

    def _show_missing_dates(self, plan: Plan, default_catalog: t.Optional[str]) -> None:
        self._add_to_dynamic_options(self.missing_dates_output)
        self.missing_dates_output.outputs = ()
        with self.missing_dates_output:
            super()._show_missing_dates(plan, default_catalog)

    def _apply(self, button: widgets.Button) -> None:
        button.disabled = True
        with button.output:
            button.plan_builder.apply()

    def _prompt_promote(self, plan_builder: PlanBuilder) -> None:
        import ipywidgets as widgets

        button = widgets.Button(
            description="Apply - Virtual Update",
            disabled=False,
            button_style="success",
            # Auto will make the button really large.
            # Likely changing this soon anyways to be just `Apply` with description above
            layout={"width": "10rem"},
        )
        self._add_to_dynamic_options(button)
        output = widgets.Output()
        self._add_to_dynamic_options(output)

        button.plan_builder = plan_builder
        button.on_click(self._apply)
        button.output = output

    def _prompt_effective_from(
        self, plan_builder: PlanBuilder, auto_apply: bool, default_catalog: t.Optional[str]
    ) -> None:
        import ipywidgets as widgets

        prompt = widgets.VBox()

        def effective_from_change_callback(change: t.Dict[str, datetime.datetime]) -> None:
            plan_builder.set_effective_from(change["new"])
            self._show_options_after_categorization(plan_builder, auto_apply, default_catalog)

        def going_forward_change_callback(change: t.Dict[str, bool]) -> None:
            checked = change["new"]
            plan_builder.set_effective_from(None if checked else yesterday_ds())
            self._show_options_after_categorization(
                plan_builder, auto_apply=auto_apply, default_catalog=default_catalog
            )

        date_picker = widgets.DatePicker(
            disabled=plan_builder.build().effective_from is None,
            value=to_date(plan_builder.build().effective_from or yesterday_ds()),
            layout={"width": "auto"},
        )
        date_picker.observe(effective_from_change_callback, "value")

        going_forward_checkbox = widgets.Checkbox(
            value=plan_builder.build().effective_from is None,
            description="Apply Going Forward Once Deployed To Prod",
            disabled=False,
            indent=False,
        )
        going_forward_checkbox.observe(going_forward_change_callback, "value")

        add_to_layout_widget(
            prompt,
            widgets.HBox(
                [
                    widgets.Label("Effective From Date:", layout={"width": "8rem"}),
                    date_picker,
                    going_forward_checkbox,
                ]
            ),
        )

        self._add_to_dynamic_options(prompt)

    def _prompt_backfill(
        self, plan_builder: PlanBuilder, auto_apply: bool, default_catalog: t.Optional[str]
    ) -> None:
        import ipywidgets as widgets

        prompt = widgets.VBox()

        backfill_or_preview = (
            "Preview"
            if plan_builder.build().is_dev and plan_builder.build().forward_only
            else "Backfill"
        )

        def _date_picker(
            plan_builder: PlanBuilder, value: t.Any, on_change: t.Callable, disabled: bool = False
        ) -> widgets.DatePicker:
            picker = widgets.DatePicker(
                disabled=disabled,
                value=value,
                layout={"width": "auto"},
            )

            picker.observe(on_change, "value")
            return picker

        def start_change_callback(change: t.Dict[str, datetime.datetime]) -> None:
            plan_builder.set_start(change["new"])
            self._show_options_after_categorization(plan_builder, auto_apply, default_catalog)

        def end_change_callback(change: t.Dict[str, datetime.datetime]) -> None:
            plan_builder.set_end(change["new"])
            self._show_options_after_categorization(plan_builder, auto_apply, default_catalog)

        if plan_builder.is_start_and_end_allowed:
            add_to_layout_widget(
                prompt,
                widgets.HBox(
                    [
                        widgets.Label(
                            f"Start {backfill_or_preview} Date:", layout={"width": "8rem"}
                        ),
                        _date_picker(
                            plan_builder, to_date(plan_builder.build().start), start_change_callback
                        ),
                    ]
                ),
            )

            add_to_layout_widget(
                prompt,
                widgets.HBox(
                    [
                        widgets.Label(f"End {backfill_or_preview} Date:", layout={"width": "8rem"}),
                        _date_picker(
                            plan_builder,
                            to_date(plan_builder.build().end),
                            end_change_callback,
                        ),
                    ]
                ),
            )

        self._add_to_dynamic_options(prompt)

        if not auto_apply:
            button = widgets.Button(
                description=f"Apply - {backfill_or_preview} Tables",
                disabled=False,
                button_style="success",
            )
            self._add_to_dynamic_options(button)
            output = widgets.Output()
            self._add_to_dynamic_options(output)

            button.plan_builder = plan_builder
            button.on_click(self._apply)
            button.output = output

    def _show_options_after_categorization(
        self, plan_builder: PlanBuilder, auto_apply: bool, default_catalog: t.Optional[str]
    ) -> None:
        self.dynamic_options_after_categorization_output.children = ()
        self.display(self.dynamic_options_after_categorization_output)
        super()._show_options_after_categorization(plan_builder, auto_apply, default_catalog)

    def _add_to_dynamic_options(self, *widgets: widgets.Widget) -> None:
        add_to_layout_widget(self.dynamic_options_after_categorization_output, *widgets)

    def _get_snapshot_change_category(
        self,
        snapshot: Snapshot,
        plan_builder: PlanBuilder,
        auto_apply: bool,
        default_catalog: t.Optional[str],
    ) -> None:
        import ipywidgets as widgets

        choice_mapping = self._snapshot_change_choices(
            snapshot,
            plan_builder.environment_naming_info,
            default_catalog,
            use_rich_formatting=False,
        )
        choices = list(choice_mapping)
        plan_builder.set_choice(snapshot, choices[0])

        def radio_button_selected(change: t.Dict[str, t.Any]) -> None:
            plan_builder.set_choice(snapshot, choices[change["owner"].index])
            self._show_options_after_categorization(plan_builder, auto_apply, default_catalog)

        radio = widgets.RadioButtons(
            options=choice_mapping.values(),
            layout={"width": "max-content"},
            disabled=False,
        )
        radio.observe(
            radio_button_selected,
            "value",
        )
        self.display(radio)

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        import ipywidgets as widgets

        divider_length = 70
        shared_style = {
            "font-size": "11px",
            "font-weight": "bold",
            "font-family": "Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace",
        }
        if result.wasSuccessful():
            success_color = {"color": "#008000"}
            header = str(h("span", {"style": shared_style}, "-" * divider_length))
            message = str(
                h(
                    "span",
                    {"style": {**shared_style, **success_color}},
                    f"Successfully Ran {str(result.testsRun)} Tests Against {target_dialect}",
                )
            )
            footer = str(h("span", {"style": shared_style}, "=" * divider_length))
            self.display(widgets.HTML("<br>".join([header, message, footer])))
        else:
            fail_color = {"color": "#db3737"}
            fail_shared_style = {**shared_style, **fail_color}
            header = str(h("span", {"style": fail_shared_style}, "-" * divider_length))
            message = str(h("span", {"style": fail_shared_style}, "Test Failure Summary"))
            num_success = str(
                h(
                    "span",
                    {"style": fail_shared_style},
                    f"Num Successful Tests: {result.testsRun - len(result.failures) - len(result.errors)}",
                )
            )
            failure_tests = []
            for test, _ in result.failures + result.errors:
                if isinstance(test, ModelTest):
                    failure_tests.append(
                        str(
                            h(
                                "span",
                                {"style": fail_shared_style},
                                f"Failure Test: {test.model.name} {test.test_name}",
                            )
                        )
                    )
            failures = "<br>".join(failure_tests)
            footer = str(h("span", {"style": fail_shared_style}, "=" * divider_length))
            error_output = widgets.Textarea(output, layout={"height": "300px", "width": "100%"})
            test_info = widgets.HTML(
                "<br>".join([header, message, footer, num_success, failures, footer])
            )
            self.display(widgets.VBox(children=[test_info, error_output], layout={"width": "100%"}))


class CaptureTerminalConsole(TerminalConsole):
    """
    Captures the output of the terminal console so that it can be extracted out and displayed within other interfaces.
    The captured output is cleared out after it is retrieved.

    Note: `_prompt` and `_confirm` need to also be overriden to work with the custom interface if you want to use
    this console interactively.
    """

    def __init__(self, console: t.Optional[RichConsole] = None, **kwargs: t.Any) -> None:
        super().__init__(console=console, **kwargs)
        self._captured_outputs: t.List[str] = []
        self._errors: t.List[str] = []

    @property
    def captured_output(self) -> str:
        return "".join(self._captured_outputs)

    @property
    def captured_errors(self) -> str:
        return "".join(self._errors)

    def consume_captured_output(self) -> str:
        output = self.captured_output
        self.clear_captured_outputs()
        return output

    def consume_captured_errors(self) -> str:
        errors = self.captured_errors
        self.clear_captured_errors()
        return errors

    def clear_captured_outputs(self) -> None:
        self._captured_outputs = []

    def clear_captured_errors(self) -> None:
        self._errors = []

    def log_error(self, message: str) -> None:
        self._errors.append(message)
        super().log_error(message)

    def _print(self, value: t.Any, **kwargs: t.Any) -> None:
        with self.console.capture() as capture:
            self.console.print(value, **kwargs)
        self._captured_outputs.append(capture.get())


class MarkdownConsole(CaptureTerminalConsole):
    """
    A console that outputs markdown. Currently this is only configured for non-interactive use so for use cases
    where you want to display a plan or test results in markdown.
    """

    def show_model_difference_summary(
        self,
        context_diff: ContextDiff,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        no_diff: bool = True,
        ignored_snapshot_ids: t.Optional[t.Set[SnapshotId]] = None,
    ) -> None:
        """Shows a summary of the differences.

        Args:
            context_diff: The context diff to use to print the summary.
            environment_naming_info: The environment naming info to reference when printing model names
            default_catalog: The default catalog to reference when deciding to remove catalog from display names
            no_diff: Hide the actual SQL differences.
            ignored_snapshot_ids: A set of snapshot names that are ignored
        """
        ignored_snapshot_ids = ignored_snapshot_ids or set()
        if context_diff.is_new_environment:
            self._print(
                f"**New environment `{context_diff.environment}` will be created from `{context_diff.create_from}`**\n\n"
            )
            if not context_diff.has_snapshot_changes:
                return

        if not context_diff.has_changes:
            self._print(f"**No differences when compared to `{context_diff.environment}`**\n\n")
            return

        self._print(f"**Summary of differences against `{context_diff.environment}`:**\n\n")

        added_snapshots = {
            context_diff.snapshots[s_id]
            for s_id in context_diff.added
            if s_id not in ignored_snapshot_ids
        }
        added_snapshot_models = {s for s in added_snapshots if s.is_model}
        if added_snapshot_models:
            self._print(f"**Added Models:**\n")
            for snapshot in added_snapshot_models:
                self._print(
                    f"- {snapshot.display_name(environment_naming_info, default_catalog)}\n"
                )
            self._print("\n")

        added_snapshot_audits = {s for s in added_snapshots if s.is_audit}
        if added_snapshot_audits:
            self._print(f"**Added Standalone Audits:**\n")
            for snapshot in added_snapshot_audits:
                self._print(
                    f"- {snapshot.display_name(environment_naming_info, default_catalog)}\n"
                )
            self._print("\n")

        removed_snapshot_table_infos = {
            snapshot_table_info
            for s_id, snapshot_table_info in context_diff.removed_snapshots.items()
            if s_id not in ignored_snapshot_ids
        }
        removed_model_snapshot_table_infos = {s for s in removed_snapshot_table_infos if s.is_model}
        if removed_model_snapshot_table_infos:
            self._print(f"**Removed Models:**\n")
            for snapshot_table_info in removed_model_snapshot_table_infos:
                self._print(
                    f"- {snapshot_table_info.display_name(environment_naming_info, default_catalog)}\n"
                )
            self._print("\n")

        removed_audit_snapshot_table_infos = {s for s in removed_snapshot_table_infos if s.is_audit}
        if removed_audit_snapshot_table_infos:
            self._print(f"**Removed Standalone Audits:**\n")
            for snapshot_table_info in removed_audit_snapshot_table_infos:
                self._print(
                    f"- {snapshot_table_info.display_name(environment_naming_info, default_catalog)}\n"
                )
            self._print("\n")

        modified_snapshots = {
            current_snapshot
            for current_snapshot, _ in context_diff.modified_snapshots.values()
            if current_snapshot.snapshot_id not in ignored_snapshot_ids
        }
        if modified_snapshots:
            directly_modified = []
            indirectly_modified = []
            metadata_modified = []
            for snapshot in modified_snapshots:
                if context_diff.directly_modified(snapshot.name):
                    directly_modified.append(snapshot)
                elif context_diff.indirectly_modified(snapshot.name):
                    indirectly_modified.append(snapshot)
                elif context_diff.metadata_updated(snapshot.name):
                    metadata_modified.append(snapshot)
            if directly_modified:
                self._print(f"**Directly Modified:**\n")
                for snapshot in directly_modified:
                    self._print(
                        f"- `{snapshot.display_name(environment_naming_info, default_catalog)}`\n"
                    )
                    if not no_diff:
                        self._print(f"```diff\n{context_diff.text_diff(snapshot.name)}\n```\n")
                self._print("\n")
            if indirectly_modified:
                self._print(f"**Indirectly Modified:**\n")
                for snapshot in indirectly_modified:
                    self._print(
                        f"- `{snapshot.display_name(environment_naming_info, default_catalog)}`\n"
                    )
                self._print("\n")
            if metadata_modified:
                self._print(f"**Metadata Updated:**\n")
                for snapshot in metadata_modified:
                    self._print(
                        f"- `{snapshot.display_name(environment_naming_info, default_catalog)}`\n"
                    )
                self._print("\n")
        if ignored_snapshot_ids:
            self._print(f"**Ignored Models (Expected Plan Start):**\n")
            for s_id in ignored_snapshot_ids:
                snapshot = context_diff.snapshots[s_id]
                self._print(
                    f"- `{snapshot.display_name(environment_naming_info, default_catalog)}` ({snapshot.get_latest(start_date(snapshot, context_diff.snapshots.values()))})\n"
                )
            self._print("\n")

    def _show_missing_dates(self, plan: Plan, default_catalog: t.Optional[str]) -> None:
        """Displays the models with missing dates."""
        missing_intervals = plan.missing_intervals
        if not missing_intervals:
            return
        self._print("**Models needing backfill (missing dates):**\n\n")
        for missing in missing_intervals:
            snapshot = plan.context_diff.snapshots[missing.snapshot_id]
            if not snapshot.is_model:
                continue

            preview_modifier = ""
            if not plan.deployability_index.is_deployable(snapshot):
                preview_modifier = " (**preview**)"

            self._print(
                f"* `{snapshot.display_name(plan.environment_naming_info, default_catalog)}`: {missing.format_intervals(snapshot.node.interval_unit)}{preview_modifier}\n"
            )
        self._print("\n")

    def _show_categorized_snapshots(self, plan: Plan, default_catalog: t.Optional[str]) -> None:
        context_diff = plan.context_diff
        for snapshot in plan.categorized:
            if not context_diff.directly_modified(snapshot.name):
                continue

            category_str = SNAPSHOT_CHANGE_CATEGORY_STR[snapshot.change_category]
            tree = Tree(
                f"[bold][direct]Directly Modified: {snapshot.display_name(plan.environment_naming_info, default_catalog)} ({category_str})"
            )
            indirect_tree = None
            for child_sid in sorted(plan.indirectly_modified.get(snapshot.snapshot_id, set())):
                child_snapshot = context_diff.snapshots[child_sid]
                if not indirect_tree:
                    indirect_tree = Tree(f"[indirect]Indirectly Modified Children:")
                    tree.add(indirect_tree)
                child_category_str = SNAPSHOT_CHANGE_CATEGORY_STR[child_snapshot.change_category]
                indirect_tree.add(
                    f"[indirect]{child_snapshot.display_name(plan.environment_naming_info, default_catalog)} ({child_category_str})"
                )
            self._print(f"```diff\n{context_diff.text_diff(snapshot.name)}\n```\n")
            self._print("```\n")
            self._print(tree)
            self._print("\n```")

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        # import ipywidgets as widgets
        if result.wasSuccessful():
            self._print(
                f"**Successfully Ran `{str(result.testsRun)}` Tests Against `{target_dialect}`**\n\n"
            )
        else:
            self._print(
                f"**Num Successful Tests: {result.testsRun - len(result.failures) - len(result.errors)}**\n\n"
            )
            for test, _ in result.failures + result.errors:
                if isinstance(test, ModelTest):
                    self._print(f"* Failure Test: `{test.model.name}` - `{test.test_name}`\n\n")
            self._print(f"```{output}```\n\n")

    def log_error(self, message: str) -> None:
        super().log_error(f"```\n{message}```\n\n")


class DatabricksMagicConsole(CaptureTerminalConsole):
    """
    Note: Databricks Magic Console currently does not support progress bars while a plan is being applied. The
    NotebookMagicConsole does support progress bars, but they will time out after 5 minutes of execution
    and it makes it difficult to see the progress of the plan.
    """

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self.evaluation_batch_progress: t.Dict[SnapshotId, t.Tuple[str, int]] = {}
        self.promotion_status: t.Tuple[int, int] = (0, 0)
        self.model_creation_status: t.Tuple[int, int] = (0, 0)
        self.migration_status: t.Tuple[int, int] = (0, 0)

    def _print(self, value: t.Any, **kwargs: t.Any) -> None:
        super()._print(value, **kwargs)
        for captured_output in self._captured_outputs:
            print(captured_output)
        self.clear_captured_outputs()

    def _prompt(self, message: str, **kwargs: t.Any) -> t.Any:
        self._print(message)
        return super()._prompt("", **kwargs)

    def _confirm(self, message: str, **kwargs: t.Any) -> bool:
        message = f"{message} [y/n]"
        self._print(message)
        return super()._confirm("", **kwargs)

    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        self.evaluation_batches = batches
        self.evaluation_environment_naming_info = environment_naming_info
        self.default_catalog = default_catalog

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        if not self.evaluation_batch_progress.get(snapshot.snapshot_id):
            display_name = snapshot.display_name(
                self.evaluation_environment_naming_info, self.default_catalog
            )
            self.evaluation_batch_progress[snapshot.snapshot_id] = (display_name, 0)
            print(f"Starting '{display_name}', Total batches: {self.evaluation_batches[snapshot]}")

    def update_snapshot_evaluation_progress(
        self, snapshot: Snapshot, batch_idx: int, duration_ms: t.Optional[int]
    ) -> None:
        view_name, loaded_batches = self.evaluation_batch_progress[snapshot.snapshot_id]
        total_batches = self.evaluation_batches[snapshot]

        loaded_batches += 1
        self.evaluation_batch_progress[snapshot.snapshot_id] = (view_name, loaded_batches)

        finished_loading = loaded_batches == total_batches
        status = "Loaded" if finished_loading else "Loading"
        print(f"{status} '{view_name}', Completed Batches: {loaded_batches}/{total_batches}")
        if finished_loading:
            total_finished_loading = len(
                [
                    s
                    for s, total in self.evaluation_batches.items()
                    if self.evaluation_batch_progress.get(s.snapshot_id, (None, -1))[1] == total
                ]
            )
            total = len(self.evaluation_batch_progress)
            print(f"Completed Loading {total_finished_loading}/{total} Models")

    def stop_evaluation_progress(self, success: bool = True) -> None:
        self.evaluation_batch_progress = {}
        super().stop_evaluation_progress(success)
        print(f"Loading {'succeeded' if success else 'failed'}")

    def start_creation_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        """Indicates that a new creation progress has begun."""
        self.model_creation_status = (0, total_tasks)
        print(f"Starting Creating New Model Versions")

    def update_creation_progress(self, snapshot: SnapshotInfoLike) -> None:
        """Update the snapshot creation progress."""
        num_creations, total_creations = self.model_creation_status
        num_creations += 1
        self.model_creation_status = (num_creations, total_creations)
        if num_creations % 5 == 0:
            print(f"Created New Model Versions: {num_creations}/{total_creations}")

    def stop_creation_progress(self, success: bool = True) -> None:
        """Stop the snapshot creation progress."""
        self.model_creation_status = (0, 0)
        print(f"New Model Creation {'succeeded' if success else 'failed'}")

    def start_promotion_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        """Indicates that a new snapshot promotion progress has begun."""
        self.promotion_status = (0, total_tasks)
        print(f"Virtually Updating '{environment_naming_info.name}'")

    def update_promotion_progress(self, snapshot: SnapshotInfoLike, promoted: bool) -> None:
        """Update the snapshot promotion progress."""
        num_promotions, total_promotions = self.promotion_status
        num_promotions += 1
        self.promotion_status = (num_promotions, total_promotions)
        if num_promotions % 5 == 0:
            print(f"Virtually Updated {num_promotions}/{total_promotions}")

    def stop_promotion_progress(self, success: bool = True) -> None:
        """Stop the snapshot promotion progress."""
        self.promotion_status = (0, 0)
        print(f"Virtual Update {'succeeded' if success else 'failed'}")

    def start_migration_progress(self, total_tasks: int) -> None:
        """Indicates that a new migration progress has begun."""
        self.migration_status = (0, total_tasks)
        print(f"Starting Migration")

    def update_migration_progress(self, num_tasks: int) -> None:
        """Update the migration progress."""
        num_migrations, total_migrations = self.migration_status
        num_migrations += num_tasks
        self.migration_status = (num_migrations, total_migrations)
        if num_migrations % 5 == 0:
            print(f"Migration Updated {num_migrations}/{total_migrations}")

    def stop_migration_progress(self, success: bool = True) -> None:
        """Stop the migration progress."""
        self.migration_status = (0, 0)
        print(f"Migration {'succeeded' if success else 'failed'}")


class DebuggerTerminalConsole(TerminalConsole):
    """A terminal console to use while debugging with no fluff, progress bars, etc."""

    def __init__(self, console: t.Optional[RichConsole], *args: t.Any, **kwargs: t.Any) -> None:
        self.console: RichConsole = console or srich.console

    def _write(self, msg: t.Any, *args: t.Any, **kwargs: t.Any) -> None:
        self.console.log(msg, *args, **kwargs)

    def start_plan_evaluation(self, plan: Plan) -> None:
        self._write("Starting plan", plan.plan_id)

    def stop_plan_evaluation(self) -> None:
        self._write("Stopping plan")

    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        self._write(f"Starting evaluation for {len(batches)} snapshots")

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        self._write(f"Evaluating {snapshot.name}")

    def update_snapshot_evaluation_progress(
        self, snapshot: Snapshot, batch_idx: int, duration_ms: t.Optional[int]
    ) -> None:
        self._write(f"Evaluating {snapshot.name} | batch={batch_idx} | duration={duration_ms}ms")

    def stop_evaluation_progress(self, success: bool = True) -> None:
        self._write(f"Stopping evaluation with success={success}")

    def start_creation_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        self._write(f"Starting creation for {total_tasks} snapshots")

    def update_creation_progress(self, snapshot: SnapshotInfoLike) -> None:
        self._write(f"Creating {snapshot.name}")

    def stop_creation_progress(self, success: bool = True) -> None:
        self._write(f"Stopping creation with success={success}")

    def update_cleanup_progress(self, object_name: str) -> None:
        self._write(f"Cleaning up {object_name}")

    def start_promotion_progress(
        self,
        total_tasks: int,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
    ) -> None:
        self._write(f"Starting promotion for {total_tasks} snapshots")

    def update_promotion_progress(self, snapshot: SnapshotInfoLike, promoted: bool) -> None:
        self._write(f"Promoting {snapshot.name}")

    def stop_promotion_progress(self, success: bool = True) -> None:
        self._write(f"Stopping promotion with success={success}")

    def start_migration_progress(self, total_tasks: int) -> None:
        self._write(f"Starting migration for {total_tasks} snapshots")

    def update_migration_progress(self, num_tasks: int) -> None:
        self._write(f"Migration {num_tasks}")

    def stop_migration_progress(self, success: bool = True) -> None:
        self._write(f"Stopping migration with success={success}")

    def show_model_difference_summary(
        self,
        context_diff: ContextDiff,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        no_diff: bool = True,
        ignored_snapshot_ids: t.Optional[t.Set[SnapshotId]] = None,
    ) -> None:
        self._write("Model Difference Summary:")
        for added in context_diff.new_snapshots:
            self._write(f"  Added: {added}")
        for removed in context_diff.removed_snapshots:
            self._write(f"  Removed: {removed}")
        for modified in context_diff.modified_snapshots:
            self._write(f"  Modified: {modified}")

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        self._write("Test Results:", result)

    def show_sql(self, sql: str) -> None:
        self._write(sql)

    def log_status_update(self, message: str) -> None:
        self._write(message, style="bold blue")

    def log_error(self, message: str) -> None:
        self._write(message, style="bold red")

    def log_success(self, message: str) -> None:
        self._write(message, style="bold green")

    def loading_start(self, message: t.Optional[str] = None) -> uuid.UUID:
        self._write(message)
        return uuid.uuid4()

    def loading_stop(self, id: uuid.UUID) -> None:
        self._write("Done")

    def show_schema_diff(self, schema_diff: SchemaDiff) -> None:
        self._write(schema_diff)

    def show_row_diff(self, row_diff: RowDiff, show_sample: bool = True) -> None:
        self._write(row_diff)


def get_console(**kwargs: t.Any) -> TerminalConsole | DatabricksMagicConsole | NotebookMagicConsole:
    """
    Returns the console that is appropriate for the current runtime environment.

    Note: Google Colab environment is untested and currently assumes is compatible with the base
    NotebookMagicConsole.
    """
    from sqlmesh import RuntimeEnv

    runtime_env = RuntimeEnv.get()

    runtime_env_mapping = {
        RuntimeEnv.DATABRICKS: DatabricksMagicConsole,
        RuntimeEnv.JUPYTER: NotebookMagicConsole,
        RuntimeEnv.TERMINAL: TerminalConsole,
        RuntimeEnv.GOOGLE_COLAB: NotebookMagicConsole,
        RuntimeEnv.DEBUGGER: DebuggerTerminalConsole,
    }
    rich_console_kwargs: t.Dict[str, t.Any] = {"theme": srich.theme}
    if runtime_env.is_jupyter or runtime_env.is_google_colab:
        rich_console_kwargs["force_jupyter"] = True
    return runtime_env_mapping[runtime_env](
        **{**{"console": RichConsole(**rich_console_kwargs)}, **kwargs}
    )
