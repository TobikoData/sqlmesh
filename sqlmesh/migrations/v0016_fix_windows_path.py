"""Fix paths that have a Windows forward slash in them."""
import json

import pandas as pd
from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = f"{schema}._snapshots"

    new_snapshots = []

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table)
    ):
        parsed_snapshot = json.loads(snapshot)
        model = parsed_snapshot["model"]
        python_env = model.get("python_env")
        if python_env:
            for py_definition in python_env.values():
                path = py_definition.get("path")
                if path:
                    py_definition["path"] = path.replace("\\", "/")

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
            }
        )

    if new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types={
                "name": exp.DataType.build("text"),
                "identifier": exp.DataType.build("text"),
                "version": exp.DataType.build("text"),
                "snapshot": exp.DataType.build("text"),
                "kind_name": exp.DataType.build("text"),
            },
            contains_json=True,
        )
