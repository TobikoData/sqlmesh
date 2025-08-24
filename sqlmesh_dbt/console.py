import typing as t
from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.model import Model
from rich.tree import Tree


class DbtCliConsole(TerminalConsole):
    def print(self, msg: str) -> None:
        return self._print(msg)

    def list_models(
        self, models: t.List[Model], list_parents: bool = True, list_audits: bool = True
    ) -> None:
        model_list = Tree("[bold]Models in project:[/bold]")

        for model in models:
            model_tree = model_list.add(model.name)

            if list_parents:
                for parent in model.depends_on:
                    model_tree.add(f"depends_on: {parent}")

            if list_audits:
                for audit_name in model.audit_definitions:
                    model_tree.add(f"audit: {audit_name}")

        self._print(model_list)
