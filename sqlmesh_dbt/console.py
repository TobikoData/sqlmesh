import typing as t
from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.model import Model
from sqlmesh.core.snapshot.definition import Node
from rich.tree import Tree


class DbtCliConsole(TerminalConsole):
    def print(self, msg: str) -> None:
        return self._print(msg)

    def list_models(
        self,
        models: t.List[Model],
        all_nodes: t.Dict[str, Node],
        list_parents: bool = True,
        list_audits: bool = True,
    ) -> None:
        model_list = Tree("[bold]Models in project:[/bold]")

        for model in models:
            model_tree = model_list.add(model.dbt_fqn or model.name)

            if list_parents:
                for parent_name in model.depends_on:
                    if parent := all_nodes.get(parent_name):
                        parent_name = parent.dbt_fqn or parent_name

                    model_tree.add(f"depends_on: {parent_name}")

            if list_audits:
                for audit_name, audit in model.audit_definitions.items():
                    model_tree.add(f"audit: {audit.dbt_fqn or audit_name}")

        self._print(model_list)
