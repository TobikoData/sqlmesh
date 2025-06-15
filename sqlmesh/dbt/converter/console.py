from __future__ import annotations
import typing as t
from pathlib import Path
from rich.console import Console as RichConsole
from rich.tree import Tree
from rich.progress import Progress, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn
from sqlmesh.core.console import PROGRESS_BAR_WIDTH
from sqlmesh.utils import columns_to_types_all_known
from sqlmesh.utils import rich as srich
import logging
from rich.prompt import Confirm

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from sqlmesh.dbt.converter.convert import ConversionReport


def make_progress_bar(
    console: t.Optional[RichConsole] = None,
    justify: t.Literal["default", "left", "center", "right", "full"] = "right",
) -> Progress:
    return Progress(
        TextColumn("[bold blue]{task.description}", justify=justify),
        BarColumn(bar_width=PROGRESS_BAR_WIDTH),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        MofNCompleteColumn(),
        "•",
        TimeElapsedColumn(),
        console=console,
    )


class DbtConversionConsole:
    """Console for displaying DBT project conversion progress"""

    def __init__(self, console: t.Optional[RichConsole] = None) -> None:
        self.console: RichConsole = console or srich.console

    def log_message(self, message: str) -> None:
        self.console.print(message)

    def start_project_conversion(self, input_path: Path) -> None:
        self.log_message(f"DBT project loaded from {input_path}; starting conversion")

    def prompt_clear_directory(self, prefix: str, path: Path) -> bool:
        return Confirm.ask(
            f"{prefix}'{path}' is not empty.\nWould you like to clear it?", console=self.console
        )

    # Models
    def start_models_conversion(self, model_count: int) -> None:
        self.progress_bar = make_progress_bar(justify="left", console=self.console)
        self.progress_bar.start()
        self.models_progress_task_id = self.progress_bar.add_task(
            "Converting models", total=model_count
        )

    def start_model_conversion(self, model_name: str) -> None:
        logger.debug(f"Converting model {model_name}")
        self.progress_bar.update(self.models_progress_task_id, description=None, refresh=True)

    def complete_model_conversion(self) -> None:
        self.progress_bar.update(self.models_progress_task_id, refresh=True, advance=1)

    def complete_models_conversion(self) -> None:
        self.progress_bar.update(self.models_progress_task_id, description=None, refresh=True)

    # Audits

    def start_audits_conversion(self, audit_count: int) -> None:
        self.audits_progress_task_id = self.progress_bar.add_task(
            "Converting audits", total=audit_count
        )

    def start_audit_conversion(self, audit_name: str) -> None:
        self.progress_bar.update(self.audits_progress_task_id, description=None, refresh=True)

    def complete_audit_conversion(self) -> None:
        self.progress_bar.update(self.audits_progress_task_id, refresh=True, advance=1)

    def complete_audits_conversion(self) -> None:
        self.progress_bar.update(self.audits_progress_task_id, description=None, refresh=True)

    # Macros

    def start_macros_conversion(self, macro_count: int) -> None:
        self.macros_progress_task_id = self.progress_bar.add_task(
            "Converting macros", total=macro_count
        )

    def start_macro_conversion(self, macro_name: str) -> None:
        self.progress_bar.update(self.macros_progress_task_id, description=None, refresh=True)

    def complete_macro_conversion(self) -> None:
        self.progress_bar.update(self.macros_progress_task_id, refresh=True, advance=1)

    def complete_macros_conversion(self) -> None:
        self.progress_bar.update(self.macros_progress_task_id, description=None, refresh=True)
        self.progress_bar.stop()

    def output_report(self, report: ConversionReport) -> None:
        tree = Tree(
            "[blue]The following models are self-referencing and their column types could not be statically inferred:"
        )

        for output_path, model in report.self_referencing_models:
            if not model.columns_to_types or not columns_to_types_all_known(model.columns_to_types):
                tree_node = tree.add(f"[green]{model.name}")
                tree_node.add(output_path.as_posix())

        self.console.print(tree)

        self.log_message(
            "[red]These will need to be manually fixed.[/red]\nEither specify the column types in the MODEL block or ensure the outer SELECT lists all columns"
        )
