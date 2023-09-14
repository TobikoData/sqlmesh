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
    SnapshotInfoLike,
    start_date,
)
from sqlmesh.core.test import ModelTest
from sqlmesh.utils import rich as srich
from sqlmesh.utils.date import to_date, yesterday_ds

if t.TYPE_CHECKING:
    import ipywidgets as widgets

    from sqlmesh.core.context_diff import ContextDiff
    from sqlmesh.core.plan import Plan
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
    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
    ) -> None:
        """Indicates that a new snapshot evaluation progress has begun."""

    @abc.abstractmethod
    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        """Starts the snapshot evaluation progress."""

    @abc.abstractmethod
    def update_snapshot_evaluation_progress(self, snapshot: Snapshot, num_batches: int) -> None:
        """Updates the snapshot evaluation progress."""

    @abc.abstractmethod
    def stop_evaluation_progress(self, success: bool = True) -> None:
        """Stops the snapshot evaluation progress."""

    @abc.abstractmethod
    def start_creation_progress(self, total_tasks: int) -> None:
        """Indicates that a new snapshot creation progress has begun."""

    @abc.abstractmethod
    def update_creation_progress(self, num_tasks: int) -> None:
        """Update the snapshot creation progress."""

    @abc.abstractmethod
    def stop_creation_progress(self, success: bool = True) -> None:
        """Stop the snapshot creation progress."""

    @abc.abstractmethod
    def start_promotion_progress(self, environment: str, total_tasks: int) -> None:
        """Indicates that a new snapshot promotion progress has begun."""

    @abc.abstractmethod
    def update_promotion_progress(self, num_tasks: int) -> None:
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
        detailed: bool = False,
        ignored_snapshot_names: t.Optional[t.Set[str]] = None,
    ) -> None:
        """Displays a summary of differences for the given models."""

    @abc.abstractmethod
    def plan(self, plan: Plan, auto_apply: bool) -> None:
        """The main plan flow.

        The console should present the user with choices on how to backfill and version the snapshots
        of a plan.

        Args:
            plan: The plan to make choices for.
            auto_apply: Whether to automatically apply the plan after all choices have been made.
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

    def __init__(self, console: t.Optional[RichConsole] = None, **kwargs: t.Any) -> None:
        self.console: RichConsole = console or srich.console

        self.evaluation_progress_live: t.Optional[Live] = None
        self.evaluation_total_progress: t.Optional[Progress] = None
        self.evaluation_total_task: t.Optional[TaskID] = None
        self.evaluation_model_progress: t.Optional[Progress] = None
        self.evaluation_model_tasks: t.Dict[str, TaskID] = {}
        self.evaluation_model_batches: t.Dict[Snapshot, int] = {}
        self.evaluation_environment_naming_info = EnvironmentNamingInfo()

        self.creation_progress: t.Optional[Progress] = None
        self.creation_task: t.Optional[TaskID] = None

        self.promotion_progress: t.Optional[Progress] = None
        self.promotion_task: t.Optional[TaskID] = None

        self.migration_progress: t.Optional[Progress] = None
        self.migration_task: t.Optional[TaskID] = None

        self.loading_status: t.Dict[uuid.UUID, Status] = {}

    def _print(self, value: t.Any, **kwargs: t.Any) -> None:
        self.console.print(value, **kwargs)

    def _prompt(self, message: str, **kwargs: t.Any) -> t.Any:
        return Prompt.ask(message, console=self.console, **kwargs)

    def _confirm(self, message: str, **kwargs: t.Any) -> bool:
        return Confirm.ask(message, console=self.console, **kwargs)

    def start_evaluation_progress(
        self,
        batches: t.Dict[Snapshot, int],
        environment_naming_info: EnvironmentNamingInfo,
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
            self.evaluation_environment_naming_info = environment_naming_info

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        if self.evaluation_model_progress and snapshot.name not in self.evaluation_model_tasks:
            view_name = snapshot.qualified_view_name.for_environment(
                self.evaluation_environment_naming_info
            )
            self.evaluation_model_tasks[snapshot.name] = self.evaluation_model_progress.add_task(
                f"Evaluating {view_name}...",
                view_name=view_name,
                total=self.evaluation_model_batches[snapshot],
            )

    def update_snapshot_evaluation_progress(self, snapshot: Snapshot, num_batches: int) -> None:
        """Update the snapshot evaluation progress."""
        if self.evaluation_total_progress and self.evaluation_model_progress:
            self.evaluation_total_progress.update(
                self.evaluation_total_task or TaskID(0), refresh=True, advance=num_batches
            )

            model_task_id = self.evaluation_model_tasks[snapshot.name]
            self.evaluation_model_progress.update(model_task_id, refresh=True, advance=num_batches)
            if (
                self.evaluation_model_progress._tasks[model_task_id].completed
                >= self.evaluation_model_batches[snapshot]
            ):
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
        self.evaluation_environment_naming_info = EnvironmentNamingInfo()

    def start_creation_progress(self, total_tasks: int) -> None:
        """Indicates that a new creation progress has begun."""
        if self.creation_progress is None:
            self.creation_progress = Progress(
                TextColumn("[bold blue]Creating new model versions", justify="right"),
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
                "Creating new model versions...",
                total=total_tasks,
            )

    def update_creation_progress(self, num_tasks: int) -> None:
        """Update the snapshot creation progress."""
        if self.creation_progress is not None and self.creation_task is not None:
            self.creation_progress.update(self.creation_task, refresh=True, advance=num_tasks)

    def stop_creation_progress(self, success: bool = True) -> None:
        """Stop the snapshot creation progress."""
        self.creation_task = None
        if self.creation_progress is not None:
            self.creation_progress.stop()
            self.creation_progress = None
            if success:
                self.log_success("All model versions have been created successfully")

    def start_promotion_progress(self, environment: str, total_tasks: int) -> None:
        """Indicates that a new snapshot promotion progress has begun."""
        if self.promotion_progress is None:
            self.promotion_progress = Progress(
                TextColumn(f"[bold blue]Virtually Updating '{environment}'", justify="right"),
                BarColumn(bar_width=40),
                "[progress.percentage]{task.percentage:>3.1f}%",
                "•",
                TimeElapsedColumn(),
                console=self.console,
            )

            self.promotion_progress.start()
            self.promotion_task = self.promotion_progress.add_task(
                f"Virtually Updating {environment}...",
                total=total_tasks,
            )

    def update_promotion_progress(self, num_tasks: int) -> None:
        """Update the snapshot promotion progress."""
        if self.promotion_progress is not None and self.promotion_task is not None:
            self.promotion_progress.update(self.promotion_task, refresh=True, advance=num_tasks)

    def stop_promotion_progress(self, success: bool = True) -> None:
        """Stop the snapshot promotion progress."""
        self.promotion_task = None
        if self.promotion_progress is not None:
            self.promotion_progress.stop()
            self.promotion_progress = None
            if success:
                self.log_success("The target environment has been updated successfully")

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
        detailed: bool = False,
        ignored_snapshot_names: t.Optional[t.Set[str]] = None,
    ) -> None:
        """Shows a summary of the differences.

        Args:
            context_diff: The context diff to use to print the summary
            detailed: Show the actual SQL differences if True.
            ignored_snapshot_names: A set of snapshot names that are ignored
        """
        ignored_snapshot_names = ignored_snapshot_names or set()
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
            detailed=detailed,
            ignored_names=ignored_snapshot_names,
        )
        self._show_summary_tree_for(
            context_diff,
            "Standalone Audits",
            lambda x: x.is_audit,
            detailed=detailed,
            ignored_names=ignored_snapshot_names,
        )

    def plan(self, plan: Plan, auto_apply: bool) -> None:
        """The main plan flow.

        The console should present the user with choices on how to backfill and version the snapshots
        of a plan.

        Args:
            plan: The plan to make choices for.
            auto_apply: Whether to automatically apply the plan after all choices have been made.
        """
        self._prompt_categorize(plan, auto_apply)
        self._show_options_after_categorization(plan, auto_apply)

        if auto_apply:
            plan.apply()

    def _get_ignored_tree(
        self, ignored_snapshot_names: t.Set[str], snapshots: t.Dict[str, Snapshot]
    ) -> Tree:
        ignored = Tree(f"[bold][ignored]Ignored Models (Expected Plan Start):")
        for model in ignored_snapshot_names:
            snapshot = snapshots[model]
            ignored.add(
                f"[ignored]{model} ({snapshot.get_latest(start_date(snapshot, snapshots.values()))})"
            )
        return ignored

    def _show_summary_tree_for(
        self,
        context_diff: ContextDiff,
        header: str,
        snapshot_selector: t.Callable[[SnapshotInfoLike], bool],
        detailed: bool = False,
        ignored_names: t.Optional[t.Set[str]] = None,
    ) -> None:
        ignored_names = ignored_names or set()
        selected_snapshots = {
            name: snapshot
            for name, snapshot in context_diff.snapshots.items()
            if snapshot_selector(snapshot)
        }
        selected_ignored_names = {name for name in selected_snapshots if name in ignored_names}
        added_names = {
            name for name in context_diff.added if snapshot_selector(context_diff.snapshots[name])
        } - selected_ignored_names
        removed_names = {
            name
            for name, snapshot in context_diff.removed_snapshots.items()
            if snapshot_selector(snapshot)
        } - selected_ignored_names
        modified_names = {
            name
            for name, snapshots in context_diff.modified_snapshots.items()
            if snapshot_selector(snapshots[0])
        } - selected_ignored_names

        tree_sets = (added_names, removed_names, modified_names, selected_ignored_names)
        if all(not names for names in tree_sets):
            return

        tree = Tree(f"[bold]{header}:")
        if added_names:
            added_tree = Tree(f"[bold][added]Added:")
            for name in added_names:
                added_tree.add(f"[added]{name}")
            tree.add(added_tree)
        if removed_names:
            removed_tree = Tree(f"[bold][removed]Removed:")
            for name in removed_names:
                removed_tree.add(f"[removed]{name}")
            tree.add(removed_tree)
        if modified_names:
            direct = Tree(f"[bold][direct]Directly Modified:")
            indirect = Tree(f"[bold][indirect]Indirectly Modified:")
            metadata = Tree(f"[bold][metadata]Metadata Updated:")
            for name in modified_names:
                if context_diff.directly_modified(name):
                    direct.add(
                        Syntax(f"{name}\n{context_diff.text_diff(name)}", "sql")
                        if detailed
                        else f"[direct]{name}"
                    )
                elif context_diff.indirectly_modified(name):
                    indirect.add(f"[indirect]{name}")
                elif context_diff.metadata_updated(name):
                    metadata.add(Syntax(f"{name}\n{context_diff.text_diff(name)}", "sql"))
                    # metadata.add(f"[metadata]{name}")
            if direct.children:
                tree.add(direct)
            if indirect.children:
                tree.add(indirect)
            if metadata.children:
                tree.add(metadata)
        if selected_ignored_names:
            tree.add(self._get_ignored_tree(selected_ignored_names, selected_snapshots))
        self._print(tree)

    def _show_options_after_categorization(self, plan: Plan, auto_apply: bool) -> None:
        if plan.forward_only and plan.new_snapshots:
            self._prompt_effective_from(plan, auto_apply)

        if plan.requires_backfill:
            self._show_missing_dates(plan)
            self._prompt_backfill(plan, auto_apply)
        elif plan.has_changes and not auto_apply:
            self._prompt_promote(plan)
        elif plan.has_unmodified_unpromoted and not auto_apply:
            self.log_status_update("\n[bold]Virtually updating unmodified models\n")
            self._prompt_promote(plan)

    def _prompt_categorize(self, plan: Plan, auto_apply: bool) -> None:
        """Get the user's change category for the directly modified models."""
        self.show_model_difference_summary(
            plan.context_diff, ignored_snapshot_names=plan.ignored_snapshot_names
        )

        self._show_categorized_snapshots(plan)

        for snapshot in plan.uncategorized:
            self._print(Syntax(plan.context_diff.text_diff(snapshot.name), "sql"))
            tree = Tree(f"[bold][direct]Directly Modified: {snapshot.name}")
            indirect_tree = None

            for child in plan.indirectly_modified[snapshot.name]:
                if not indirect_tree:
                    indirect_tree = Tree(f"[indirect]Indirectly Modified Children:")
                    tree.add(indirect_tree)
                indirect_tree.add(f"[indirect]{child}")
            self._print(tree)
            self._get_snapshot_change_category(snapshot, plan, auto_apply)

    def _show_categorized_snapshots(self, plan: Plan) -> None:
        context_diff = plan.context_diff
        for snapshot in plan.categorized:
            if not context_diff.directly_modified(snapshot.name):
                continue

            category_str = SNAPSHOT_CHANGE_CATEGORY_STR[snapshot.change_category]
            tree = Tree(f"[bold][direct]Directly Modified: {snapshot.name} ({category_str})")
            syntax_dff = Syntax(context_diff.text_diff(snapshot.name), "sql")
            indirect_tree = None
            for child in plan.indirectly_modified[snapshot.name]:
                if not indirect_tree:
                    indirect_tree = Tree(f"[indirect]Indirectly Modified Children:")
                    tree.add(indirect_tree)
                child_category_str = SNAPSHOT_CHANGE_CATEGORY_STR[
                    plan.context_diff.snapshots[child].change_category
                ]
                indirect_tree.add(f"[indirect]{child} ({child_category_str})")
            self._print(syntax_dff)
            self._print(tree)

    def _show_missing_dates(self, plan: Plan) -> None:
        """Displays the models with missing dates."""
        if not plan.missing_intervals:
            return
        backfill = Tree("[bold]Models needing backfill (missing dates):")
        for missing in plan.missing_intervals:
            snapshot = plan.context_diff.snapshots[missing.snapshot_name]
            if not snapshot.is_model:
                continue
            view_name = snapshot.qualified_view_name.for_environment(plan.environment_naming_info)
            backfill.add(f"{view_name}: {missing.format_intervals(snapshot.node.interval_unit)}")
        self._print(backfill)

    def _prompt_effective_from(self, plan: Plan, auto_apply: bool) -> None:
        if not plan.effective_from:
            effective_from = self._prompt(
                "Enter the effective date (eg. '1 year', '2020-01-01') to apply forward-only changes retroactively or blank to only apply them going forward once changes are deployed to prod"
            )
            if effective_from:
                plan.effective_from = effective_from

        if plan.is_dev and plan.effective_from:
            plan.set_start(plan.effective_from)

    def _prompt_backfill(self, plan: Plan, auto_apply: bool) -> None:
        is_forward_only_dev = plan.is_dev and plan.forward_only
        backfill_or_preview = "preview" if is_forward_only_dev else "backfill"

        if plan.is_start_and_end_allowed:
            if not plan.override_start:
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
                    blank_meaning = "to backfill from the beginning of history"
                    default_start = None

                start = self._prompt(
                    f"Enter the {backfill_or_preview} start date (eg. '1 year', '2020-01-01') or blank {blank_meaning}",
                )
                if start:
                    plan.start = start
                elif default_start:
                    plan.start = default_start

            if not plan.override_end:
                end = self._prompt(
                    f"Enter the {backfill_or_preview} end date (eg. '1 month ago', '2020-01-01') or blank to {backfill_or_preview} up until now",
                )
                if end:
                    plan.end = end
        if plan.ignored_snapshot_names:
            self._print(
                self._get_ignored_tree(plan.ignored_snapshot_names, plan.context_diff.snapshots)
            )
        if not auto_apply and self._confirm(f"Apply - {backfill_or_preview.capitalize()} Tables"):
            plan.apply()

    def _prompt_promote(self, plan: Plan) -> None:
        if self._confirm(
            f"Apply - Virtual Update",
        ):
            plan.apply()

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
        self, snapshot: Snapshot, plan: Plan, auto_apply: bool
    ) -> None:
        choices = self._snapshot_change_choices(snapshot)
        response = self._prompt(
            "\n".join([f"[{i+1}] {choice}" for i, choice in enumerate(choices.values())]),
            show_choices=False,
            choices=[f"{i+1}" for i in range(len(choices))],
        )
        choice = list(choices)[int(response) - 1]
        plan.set_choice(snapshot, choice)

    def _snapshot_change_choices(
        self, snapshot: Snapshot, use_rich_formatting: bool = True
    ) -> t.Dict[SnapshotChangeCategory, str]:
        direct = snapshot.name
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

    def _show_missing_dates(self, plan: Plan) -> None:
        self._add_to_dynamic_options(self.missing_dates_output)
        self.missing_dates_output.outputs = ()
        with self.missing_dates_output:
            super()._show_missing_dates(plan)

    def _apply(self, button: widgets.Button) -> None:
        button.disabled = True
        with button.output:
            button.plan.apply()

    def _prompt_promote(self, plan: Plan) -> None:
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

        button.plan = plan
        button.on_click(self._apply)
        button.output = output

    def _prompt_effective_from(self, plan: Plan, auto_apply: bool) -> None:
        import ipywidgets as widgets

        prompt = widgets.VBox()

        def effective_from_change_callback(change: t.Dict[str, datetime.datetime]) -> None:
            plan.effective_from = change["new"]
            self._show_options_after_categorization(plan, auto_apply)

        def going_forward_change_callback(change: t.Dict[str, bool]) -> None:
            checked = change["new"]
            plan.effective_from = None if checked else yesterday_ds()
            self._show_options_after_categorization(plan, auto_apply=auto_apply)

        date_picker = widgets.DatePicker(
            disabled=plan.effective_from is None,
            value=to_date(plan.effective_from or yesterday_ds()),
            layout={"width": "auto"},
        )
        date_picker.observe(effective_from_change_callback, "value")

        going_forward_checkbox = widgets.Checkbox(
            value=plan.effective_from is None,
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

    def _prompt_backfill(self, plan: Plan, auto_apply: bool) -> None:
        import ipywidgets as widgets

        prompt = widgets.VBox()

        backfill_or_preview = "Preview" if plan.is_dev and plan.forward_only else "Backfill"

        def _date_picker(
            plan: Plan, value: t.Any, on_change: t.Callable, disabled: bool = False
        ) -> widgets.DatePicker:
            picker = widgets.DatePicker(
                disabled=disabled,
                value=value,
                layout={"width": "auto"},
            )

            picker.observe(on_change, "value")
            return picker

        def start_change_callback(change: t.Dict[str, datetime.datetime]) -> None:
            plan.start = change["new"]
            self._show_options_after_categorization(plan, auto_apply)

        def end_change_callback(change: t.Dict[str, datetime.datetime]) -> None:
            plan.end = change["new"]
            self._show_options_after_categorization(plan, auto_apply)

        if plan.is_start_and_end_allowed:
            add_to_layout_widget(
                prompt,
                widgets.HBox(
                    [
                        widgets.Label(
                            f"Start {backfill_or_preview} Date:", layout={"width": "8rem"}
                        ),
                        _date_picker(plan, to_date(plan.start), start_change_callback),
                    ]
                ),
            )

            add_to_layout_widget(
                prompt,
                widgets.HBox(
                    [
                        widgets.Label(f"End {backfill_or_preview} Date:", layout={"width": "8rem"}),
                        _date_picker(
                            plan,
                            to_date(plan.end),
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

            button.plan = plan
            button.on_click(self._apply)
            button.output = output

    def _show_options_after_categorization(self, plan: Plan, auto_apply: bool) -> None:
        self.dynamic_options_after_categorization_output.children = ()
        self.display(self.dynamic_options_after_categorization_output)
        super()._show_options_after_categorization(plan, auto_apply)

    def _add_to_dynamic_options(self, *widgets: widgets.Widget) -> None:
        add_to_layout_widget(self.dynamic_options_after_categorization_output, *widgets)

    def _get_snapshot_change_category(
        self, snapshot: Snapshot, plan: Plan, auto_apply: bool
    ) -> None:
        import ipywidgets as widgets

        def radio_button_selected(change: t.Dict[str, t.Any]) -> None:
            plan.set_choice(snapshot, choices[change["owner"].index])
            self._show_options_after_categorization(plan, auto_apply)

        choice_mapping = self._snapshot_change_choices(snapshot, use_rich_formatting=False)
        choices = list(choice_mapping)
        plan.set_choice(snapshot, choices[0])

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

    @property
    def captured_output(self) -> str:
        return "".join(self._captured_outputs)

    def consume_captured_output(self) -> str:
        output = self.captured_output
        self.clear_captured_outputs()
        return output

    def clear_captured_outputs(self) -> None:
        self._captured_outputs = []

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
        detailed: bool = False,
        ignored_snapshot_names: t.Optional[t.Set[str]] = None,
    ) -> None:
        """Shows a summary of the differences.

        Args:
            context_diff: The context diff to use to print the summary.
            detailed: Show the actual SQL differences if True.
            ignored_snapshot_names: A set of snapshot names that are ignored
        """
        ignored_snapshot_names = ignored_snapshot_names or set()
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

        added_model_names = {
            name for name in context_diff.added if context_diff.snapshots[name].is_model
        } - ignored_snapshot_names
        if added_model_names:
            self._print(f"**Added Models:**\n")
            for model_name in added_model_names:
                self._print(f"- {model_name}\n")
            self._print("\n")

        added_audit_names = {
            name for name in context_diff.added if context_diff.snapshots[name].is_audit
        } - ignored_snapshot_names
        if added_audit_names:
            self._print(f"**Added Standalone Audits:**\n")
            for audit_name in added_audit_names:
                self._print(f"- {audit_name}\n")
            self._print("\n")

        removed_model_names = {
            name for name, snapshot in context_diff.removed_snapshots.items() if snapshot.is_model
        } - ignored_snapshot_names
        if removed_model_names:
            self._print(f"**Removed Models:**\n")
            for model_name in removed_model_names:
                self._print(f"- {model_name}\n")
            self._print("\n")

        removed_audit_names = {
            name for name, snapshot in context_diff.removed_snapshots.items() if snapshot.is_audit
        } - ignored_snapshot_names
        if removed_audit_names:
            self._print(f"**Removed Standalone Audits:**\n")
            for audit_name in removed_audit_names:
                self._print(f"- {audit_name}\n")
            self._print("\n")

        modified_model_names = context_diff.modified_snapshots.keys() - ignored_snapshot_names
        if modified_model_names:
            directly_modified = []
            indirectly_modified = []
            metadata_modified = []
            for model_name in modified_model_names:
                if context_diff.directly_modified(model_name):
                    directly_modified.append(model_name)
                elif context_diff.indirectly_modified(model_name):
                    indirectly_modified.append(model_name)
                elif context_diff.metadata_updated(model_name):
                    metadata_modified.append(model_name)
            if directly_modified:
                self._print(f"**Directly Modified:**\n")
                for model_name in directly_modified:
                    self._print(f"- `{model_name}`\n")
                    if detailed:
                        self._print(f"```diff\n{context_diff.text_diff(model_name)}\n```\n")
                self._print("\n")
            if indirectly_modified:
                self._print(f"**Indirectly Modified:**\n")
                for model_name in indirectly_modified:
                    self._print(f"- `{model_name}`\n")
                self._print("\n")
            if metadata_modified:
                self._print(f"**Metadata Updated:**\n")
                for model_name in metadata_modified:
                    self._print(f"- `{model_name}`\n")
                self._print("\n")
        if ignored_snapshot_names:
            self._print(f"**Ignored Models (Expected Plan Start):**\n")
            for model_name in ignored_snapshot_names:
                snapshot = context_diff.snapshots[model_name]
                self._print(
                    f"- `{model_name}` ({snapshot.get_latest(start_date(snapshot, context_diff.snapshots.values()))})\n"
                )
            self._print("\n")

    def _show_missing_dates(self, plan: Plan) -> None:
        """Displays the models with missing dates."""
        if not plan.missing_intervals:
            return
        self._print("**Models needing backfill (missing dates):**\n\n")
        for missing in plan.missing_intervals:
            snapshot = plan.context_diff.snapshots[missing.snapshot_name]
            view_name = snapshot.qualified_view_name.for_environment(plan.environment_naming_info)
            self._print(
                f"* `{view_name}`: {missing.format_intervals(snapshot.node.interval_unit)}\n"
            )
        self._print("\n")

    def _show_categorized_snapshots(self, plan: Plan) -> None:
        context_diff = plan.context_diff
        for snapshot in plan.categorized:
            if not context_diff.directly_modified(snapshot.name):
                continue

            category_str = SNAPSHOT_CHANGE_CATEGORY_STR[snapshot.change_category]
            tree = Tree(f"[bold][direct]Directly Modified: {snapshot.name} ({category_str})")
            indirect_tree = None
            for child in plan.indirectly_modified[snapshot.name]:
                if not indirect_tree:
                    indirect_tree = Tree(f"[indirect]Indirectly Modified Children:")
                    tree.add(indirect_tree)
                child_category_str = SNAPSHOT_CHANGE_CATEGORY_STR[
                    plan.context_diff.snapshots[child].change_category
                ]
                indirect_tree.add(f"[indirect]{child} ({child_category_str})")
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


class DatabricksMagicConsole(CaptureTerminalConsole):
    """
    Note: Databricks Magic Console currently does not support progress bars while a plan is being applied. The
    NotebookMagicConsole does support progress bars, but they will time out after 5 minutes of execution
    and it makes it difficult to see the progress of the plan.
    """

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self.evaluation_batch_progress: t.Dict[str, t.Tuple[str, int]] = {}
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
    ) -> None:
        self.evaluation_batches = batches
        self.evaluation_environment_naming_info = environment_naming_info

    def start_snapshot_evaluation_progress(self, snapshot: Snapshot) -> None:
        if not self.evaluation_batch_progress.get(snapshot.name):
            view_name = snapshot.qualified_view_name.for_environment(
                self.evaluation_environment_naming_info
            )
            self.evaluation_batch_progress[snapshot.name] = (view_name, 0)
            print(f"Starting '{view_name}', Total batches: {self.evaluation_batches[snapshot]}")

    def update_snapshot_evaluation_progress(self, snapshot: Snapshot, num_batches: int) -> None:
        view_name, loaded_batches = self.evaluation_batch_progress[snapshot.name]
        total_batches = self.evaluation_batches[snapshot]

        loaded_batches += num_batches
        self.evaluation_batch_progress[snapshot.name] = (view_name, loaded_batches)

        finished_loading = loaded_batches == total_batches
        status = "Loaded" if finished_loading else "Loading"
        print(f"{status} '{view_name}', Completed Batches: {loaded_batches}/{total_batches}")
        if finished_loading:
            total_finished_loading = len(
                [
                    s
                    for s, total in self.evaluation_batches.items()
                    if self.evaluation_batch_progress.get(s.name, (None, -1))[1] == total
                ]
            )
            total = len(self.evaluation_batch_progress)
            print(f"Completed Loading {total_finished_loading}/{total} Models")

    def stop_evaluation_progress(self, success: bool = True) -> None:
        self.evaluation_batch_progress = {}
        super().stop_evaluation_progress(success)
        print(f"Loading {'succeeded' if success else 'failed'}")

    def start_creation_progress(self, total_tasks: int) -> None:
        """Indicates that a new creation progress has begun."""
        self.model_creation_status = (0, total_tasks)
        print(f"Starting Creating New Model Versions")

    def update_creation_progress(self, num_tasks: int) -> None:
        """Update the snapshot creation progress."""
        num_creations, total_creations = self.model_creation_status
        num_creations += num_tasks
        self.model_creation_status = (num_creations, total_creations)
        if num_creations % 5 == 0:
            print(f"Created New Model Versions: {num_creations}/{total_creations}")

    def stop_creation_progress(self, success: bool = True) -> None:
        """Stop the snapshot creation progress."""
        self.model_creation_status = (0, 0)
        print(f"New Model Creation {'succeeded' if success else 'failed'}")

    def start_promotion_progress(self, environment: str, total_tasks: int) -> None:
        """Indicates that a new snapshot promotion progress has begun."""
        self.promotion_status = (0, total_tasks)
        print(f"Virtually Updating '{environment}'")

    def update_promotion_progress(self, num_tasks: int) -> None:
        """Update the snapshot promotion progress."""
        num_promotions, total_promotions = self.promotion_status
        num_promotions += num_tasks
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


def get_console(**kwargs: t.Any) -> TerminalConsole | DatabricksMagicConsole | NotebookMagicConsole:
    """
    Returns the console that is appropriate for the current runtime environment.

    Note: Google Colab environment is untested and currently assumes is compatible with the base
    NotebookMagicConsole.
    """
    from sqlmesh import RuntimeEnv, runtime_env

    runtime_env_mapping = {
        RuntimeEnv.DATABRICKS: DatabricksMagicConsole,
        RuntimeEnv.JUPYTER: NotebookMagicConsole,
        RuntimeEnv.TERMINAL: TerminalConsole,
        RuntimeEnv.GOOGLE_COLAB: NotebookMagicConsole,
    }
    return runtime_env_mapping[runtime_env](**kwargs)
