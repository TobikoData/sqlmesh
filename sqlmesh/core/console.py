from __future__ import annotations

import abc
import typing as t
import unittest
import uuid

from hyperscript import h
from rich.console import Console as RichConsole
from rich.progress import BarColumn, Progress, TaskID, TextColumn, TimeElapsedColumn
from rich.prompt import Confirm, Prompt
from rich.status import Status
from rich.syntax import Syntax
from rich.tree import Tree

from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory
from sqlmesh.core.test import ModelTest
from sqlmesh.utils import rich as srich
from sqlmesh.utils.date import now, to_date

if t.TYPE_CHECKING:
    import ipywidgets as widgets

    from sqlmesh.core.context_diff import ContextDiff
    from sqlmesh.core.plan import Plan

    LayoutWidget = t.TypeVar("LayoutWidget", bound=t.Union[widgets.VBox, widgets.HBox])


SNAPSHOT_CHANGE_CATEGORY_STR = {
    SnapshotChangeCategory.BREAKING: "Breaking",
    SnapshotChangeCategory.NON_BREAKING: "Non-breaking",
    SnapshotChangeCategory.FORWARD_ONLY: "Forward-only",
}


class Console(abc.ABC):
    """Abstract base class for defining classes used for displaying information to the user and also interact
    with them when their input is needed"""

    @abc.abstractmethod
    def start_snapshot_progress(self, snapshot_name: str, total_batches: int) -> None:
        """Indicates that a new load progress has begun."""

    @abc.abstractmethod
    def update_snapshot_progress(self, snapshot_name: str, num_batches: int) -> None:
        """Update snapshot progress."""

    @abc.abstractmethod
    def complete_snapshot_progress(self) -> None:
        """Indicates that load progress is complete."""

    @abc.abstractmethod
    def stop_snapshot_progress(self) -> None:
        """Stop the load progress"""

    @abc.abstractmethod
    def show_model_difference_summary(
        self, context_diff: ContextDiff, detailed: bool = False
    ) -> None:
        """Displays a summary of differences for the given models"""

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
        """Display the test result and output

        Args:
            result: The unittest test result that contains metrics like num success, fails, ect.
            output: The generated output from the unittest
            target_dialect: The dialect that tests were run against. Assumes all tests run against the same dialect.
        """

    @abc.abstractmethod
    def show_sql(self, sql: str) -> None:
        """Display to the user SQL"""

    @abc.abstractmethod
    def log_status_update(self, message: str) -> None:
        """Display general status update to the user"""

    @abc.abstractmethod
    def log_error(self, message: str) -> None:
        """Display error info to the user"""

    @abc.abstractmethod
    def log_success(self, message: str) -> None:
        """Display a general successful message to the user"""

    @abc.abstractmethod
    def loading_start(self, message: t.Optional[str] = None) -> uuid.UUID:
        """Starts loading and returns a unique ID that can be used to stop the loading. Optionally can display a message"""

    @abc.abstractmethod
    def loading_stop(self, id: uuid.UUID) -> None:
        """Stop loading for the given id"""


class TerminalConsole(Console):
    """A rich based implementation of the console"""

    def __init__(self, console: t.Optional[RichConsole] = None) -> None:
        self.console: RichConsole = console or srich.console
        self.progress: t.Optional[Progress] = None
        self.tasks: t.Dict[str, t.Tuple[TaskID, int]] = {}
        self.loading_status: t.Dict[uuid.UUID, Status] = {}

    def start_snapshot_progress(self, snapshot_name: str, total_batches: int) -> None:
        """Indicates that a new load progress has begun."""
        if not self.progress:
            self.progress = Progress(
                TextColumn("[bold blue]{task.fields[snapshot_name]}", justify="right"),
                BarColumn(bar_width=40),
                "[progress.percentage]{task.percentage:>3.1f}%",
                "•",
                srich.SchedulerBatchColumn(),
                "•",
                TimeElapsedColumn(),
                console=self.console,
            )
            self.progress.start()
            self.tasks = {}
        self.tasks[snapshot_name] = (
            self.progress.add_task(
                f"Running {snapshot_name}...",
                snapshot_name=snapshot_name,
                total=total_batches,
            ),
            total_batches,
        )

    def update_snapshot_progress(self, snapshot_name: str, num_batches: int) -> None:
        """Update snapshot progress."""
        if self.progress and self.tasks:
            task_id = self.tasks[snapshot_name][0]
            self.progress.update(task_id, advance=num_batches)

    def complete_snapshot_progress(self) -> None:
        """Indicates that load progress is complete"""
        # Make sure all tasks are marked as completed when we're done processing the batches. The
        # refresh argument is used so that the bars are finalized properly in Jupyter notebooks.
        # See first "Note" here: https://rich.readthedocs.io/en/stable/progress.html#progress-display
        if self.progress:
            for task_id, total_batches in self.tasks.values():
                self.progress.update(task_id, refresh=True, completed=total_batches)

        self.log_success("All model batches have been executed successfully")
        self.stop_snapshot_progress()

    def stop_snapshot_progress(self) -> None:
        """Stop the load progress"""
        self.tasks = {}
        if self.progress:
            self.progress.stop()
            self.progress = None

    def show_model_difference_summary(
        self, context_diff: ContextDiff, detailed: bool = False
    ) -> None:
        """Shows a summary of the differences.

        Args:
            context_diff: The context diff to use to print the summary
            detailed: Show the actual SQL differences if True.
        """
        if not context_diff.has_differences:
            self.console.print(
                Tree(
                    f"[bold]No differences when compared to `{context_diff.environment}`"
                )
            )
            return
        tree = Tree(
            f"[bold]Summary of differences against `{context_diff.environment}`:"
        )

        if context_diff.added:
            added_tree = Tree(f"[bold][added]Added Models:")
            for model in context_diff.added:
                added_tree.add(f"[added]{model}")
            tree.add(added_tree)

        if context_diff.removed:
            removed_tree = Tree(f"[bold][removed]Removed Models:")
            for model in context_diff.removed:
                removed_tree.add(f"[removed]{model}")
            tree.add(removed_tree)

        if context_diff.modified_snapshots:
            direct = Tree(f"[bold][direct]Directly Modified:")
            indirect = Tree(f"[bold][indirect]Indirectly Modified:")
            for model in context_diff.modified_snapshots:
                if context_diff.directly_modified(model):
                    direct.add(
                        Syntax(f"{model}\n{context_diff.text_diff(model)}", "sql")
                        if detailed
                        else f"[direct]{model}"
                    )
                else:
                    indirect.add(f"[indirect]{model}")
            if direct.children:
                tree.add(direct)
            if indirect.children:
                tree.add(indirect)
        self.console.print(tree)

    def plan(self, plan: Plan, auto_apply: bool) -> None:
        """The main plan flow.

        The console should present the user with choices on how to backfill and version the snapshots
        of a plan.

        Args:
            plan: The plan to make choices for.
            auto_apply: Whether to automatically apply the plan after all choices have been made.
        """
        unbounded_end = not plan.is_dev and plan.is_unbounded_end
        self._prompt_categorize(plan, auto_apply)
        self._show_options_after_categorization(
            plan, auto_apply, unbounded_end=unbounded_end
        )

        if auto_apply:
            plan.apply()

    def _show_options_after_categorization(
        self, plan: Plan, auto_apply: bool, unbounded_end: bool = False
    ) -> None:
        if plan.requires_backfill:
            self._show_missing_dates(plan)
            self._prompt_backfill(plan, auto_apply, unbounded_end=unbounded_end)
        elif plan.context_diff.has_differences and not auto_apply:
            self._prompt_promote(plan)

    def _prompt_categorize(self, plan: Plan, auto_apply: bool) -> None:
        """Get the user's change category for the directly modified models"""
        self.show_model_difference_summary(plan.context_diff)

        self._show_categorized_snapshots(plan)

        for snapshot in plan.uncategorized:
            self.console.print(
                Syntax(plan.context_diff.text_diff(snapshot.name), "sql")
            )
            tree = Tree(f"[bold][direct]Directly Modified: {snapshot.name}")
            indirect_tree = None

            for child in plan.indirectly_modified[snapshot.name]:
                if not indirect_tree:
                    indirect_tree = Tree(f"[indirect]Indirectly Modified Children:")
                    tree.add(indirect_tree)
                indirect_tree.add(f"[indirect]{child}")
            self.console.print(tree)
            self._get_snapshot_change_category(snapshot, plan, auto_apply)

    def _show_categorized_snapshots(self, plan: Plan) -> None:
        context_diff = plan.context_diff
        for snapshot in plan.categorized:
            if not context_diff.directly_modified(snapshot.name):
                continue

            category_str = SNAPSHOT_CHANGE_CATEGORY_STR[
                plan.snapshot_change_category(snapshot)
            ]
            tree = Tree(
                f"[bold][direct]Directly Modified: {snapshot.name} ({category_str})"
            )
            syntax_dff = Syntax(context_diff.text_diff(snapshot.name), "sql")
            indirect_tree = None
            for child in plan.indirectly_modified[snapshot.name]:
                if not indirect_tree:
                    indirect_tree = Tree(f"[indirect]Indirectly Modified Children:")
                    tree.add(indirect_tree)
                indirect_tree.add(f"[indirect]{child}")
            self.console.print(syntax_dff)
            self.console.print(tree)

    def _show_missing_dates(self, plan: Plan) -> None:
        """Displays the models with missing dates"""
        if not plan.missing_intervals:
            return
        backfill = Tree("[bold]Models needing backfill (missing dates):")
        for missing in plan.missing_intervals:
            backfill.add(f"{missing.snapshot_name}: {missing.format_missing_range()}")
        self.console.print(backfill)

    def _prompt_backfill(
        self, plan: Plan, auto_apply: bool, unbounded_end: bool = False
    ) -> None:
        is_forward_only_dev = plan.is_dev and plan.forward_only
        backfill_or_preview = "preview" if is_forward_only_dev else "backfill"

        if not plan.override_start:
            blank_meaning = (
                "to preview starting from yesterday"
                if is_forward_only_dev
                else "for the beginning of history"
            )
            start = Prompt.ask(
                f"Enter the {backfill_or_preview} start date (eg. '1 year', '2020-01-01') or blank {blank_meaning}",
                console=self.console,
            )
            if start:
                plan.start = start
        if not plan.override_end:
            blank_meaning = (
                "to preview up until now" if is_forward_only_dev else "if unbounded"
            )
            end = Prompt.ask(
                f"Enter the {backfill_or_preview} end date (eg. '1 month ago', '2020-01-01') or blank {blank_meaning}",
                console=self.console,
            )
            if end:
                plan.end = end

        if not auto_apply and Confirm.ask(
            f"Apply - {backfill_or_preview.capitalize()} Tables"
        ):
            plan.apply()

    def _prompt_promote(self, plan: Plan) -> None:
        if Confirm.ask(
            f"Apply - Logical Update",
            console=self.console,
        ):
            plan.apply()

    def log_test_results(
        self, result: unittest.result.TestResult, output: str, target_dialect: str
    ) -> None:
        divider_length = 70
        if result.wasSuccessful():
            self.console.print("=" * divider_length)
            self.console.print(
                f"Successfully Ran {str(result.testsRun)} tests against {target_dialect}",
                style="green",
            )
            self.console.print("-" * divider_length)
        else:
            self.console.print("-" * divider_length)
            self.console.print("Test Failure Summary")
            self.console.print("=" * divider_length)
            self.console.print(
                f"Num Successful Tests: {result.testsRun - len(result.failures) - len(result.errors)}"
            )
            for test, _ in result.failures + result.errors:
                if isinstance(test, ModelTest):
                    self.console.print(
                        f"Failure Test: {test.model_name} {test.test_name}"
                    )
            self.console.print("=" * divider_length)
            self.console.print(output)

    def show_sql(self, sql: str) -> None:
        self.console.print(Syntax(sql, "sql"))

    def log_status_update(self, message: str) -> None:
        self.console.print(message)

    def log_error(self, message: str) -> None:
        self.console.print(f"[red]{message}[/red]")

    def log_success(self, message: str) -> None:
        self.console.print(f"\n[green]{message}[/green]\n")

    def loading_start(self, message: t.Optional[str] = None) -> uuid.UUID:
        id = uuid.uuid4()
        self.loading_status[id] = Status(
            message or "", console=self.console, spinner="line"
        )
        self.loading_status[id].start()
        return id

    def loading_stop(self, id: uuid.UUID) -> None:
        self.loading_status[id].stop()
        del self.loading_status[id]

    def _get_snapshot_change_category(
        self, snapshot: Snapshot, plan: Plan, auto_apply: bool
    ) -> None:
        choices = self._snapshot_change_choices(snapshot)
        response = Prompt.ask(
            "\n".join(
                [f"[{i+1}] {choice}" for i, choice in enumerate(choices.values())]
            ),
            console=self.console,
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
        if snapshot.is_view_kind:
            choices = {
                SnapshotChangeCategory.BREAKING: f"Update {direct} and backfill {indirect}",
                SnapshotChangeCategory.NON_BREAKING: f"Update {direct} but don't backfill {indirect}",
            }
        elif snapshot.is_embedded_kind:
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


def add_to_layout_widget(
    target_widget: LayoutWidget, *widgets: widgets.Widget
) -> LayoutWidget:
    """Helper function to add a widget to a layout widget
    Args:
        target_widget: The layout widget to add the other widget(s) to
        *widgets: The widgets to add to the layout widget

    Returns:
        The layout widget with the children added
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
        self, display: t.Callable, console: t.Optional[RichConsole] = None
    ) -> None:
        import ipywidgets as widgets

        super().__init__(console)
        self.display = display
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
            description="Apply - Logical Update",
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

    def _prompt_backfill(
        self, plan: Plan, auto_apply: bool, unbounded_end: bool = False
    ) -> None:
        import ipywidgets as widgets

        prompt = widgets.VBox()

        backfill_or_preview = (
            "Preview" if plan.is_dev and plan.forward_only else "Backfill"
        )

        def _date_picker(
            plan: Plan, value: t.Any, on_change: t.Callable, disabled: bool = False
        ):
            picker = widgets.DatePicker(
                disabled=disabled,
                value=value,
                layout={"width": "auto"},
            )

            picker.observe(on_change, "value")
            return picker

        def _checkbox(description: str, value: bool, on_change: t.Callable):
            checkbox = widgets.Checkbox(
                value=value,
                description=description,
                disabled=False,
                indent=False,
            )

            checkbox.observe(on_change, "value")
            return checkbox

        def start_change_callback(change):
            plan.start = change["new"]
            self._show_options_after_categorization(
                plan, auto_apply, unbounded_end=unbounded_end
            )

        def end_change_callback(change):
            plan.end = change["new"]
            self._show_options_after_categorization(
                plan, auto_apply, unbounded_end=unbounded_end
            )

        def unbounded_end_callback(change):
            checked = change["new"]
            if checked:
                plan.end = None
            else:
                plan.end = now()
            self._show_options_after_categorization(
                plan, auto_apply, unbounded_end=checked
            )

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

        unbounded_end_date_widget = (
            [_checkbox("Unbounded End Date", unbounded_end, unbounded_end_callback)]
            if not plan.is_dev
            else []
        )

        add_to_layout_widget(
            prompt,
            widgets.HBox(
                [
                    widgets.Label(
                        f"End {backfill_or_preview} Date:", layout={"width": "8rem"}
                    ),
                    _date_picker(
                        plan,
                        to_date(plan.end),
                        end_change_callback,
                        disabled=unbounded_end,
                    ),
                    *unbounded_end_date_widget,
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

    def _show_options_after_categorization(
        self, plan: Plan, auto_apply: bool, unbounded_end: bool = False
    ) -> None:
        self.dynamic_options_after_categorization_output.children = ()
        self.display(self.dynamic_options_after_categorization_output)
        super()._show_options_after_categorization(
            plan, auto_apply, unbounded_end=unbounded_end
        )

    def _add_to_dynamic_options(self, *widgets: widgets.Widget) -> None:
        add_to_layout_widget(self.dynamic_options_after_categorization_output, *widgets)

    def _get_snapshot_change_category(
        self, snapshot: Snapshot, plan: Plan, auto_apply: bool
    ) -> None:
        import ipywidgets as widgets

        def radio_button_selected(change):
            plan.set_choice(snapshot, choices[change["owner"].index])
            self._show_options_after_categorization(plan, auto_apply)

        choice_mapping = self._snapshot_change_choices(
            snapshot, use_rich_formatting=False
        )
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
            message = str(
                h("span", {"style": fail_shared_style}, "Test Failure Summary")
            )
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
                                f"Failure Test: {test.model_name} {test.test_name}",
                            )
                        )
                    )
            failures = "<br>".join(failure_tests)
            footer = str(h("span", {"style": fail_shared_style}, "=" * divider_length))
            error_output = widgets.Textarea(
                output, layout={"height": "300px", "width": "100%"}
            )
            test_info = widgets.HTML(
                "<br>".join([header, message, footer, num_success, failures, footer])
            )
            self.display(
                widgets.VBox(
                    children=[test_info, error_output], layout={"width": "100%"}
                )
            )


def get_console() -> TerminalConsole:
    """
    Currently we only return TerminalConsole since the MagicConsole is only referenced in the magics and
    called directly. Seems reasonable we will want dynamic consoles in the future based on runtime environment
    so going to leave this for now.
    """
    return TerminalConsole()
