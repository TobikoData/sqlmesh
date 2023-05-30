"""Remove pre- / post- hooks from existing snapshots."""
import json

import pandas as pd
from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = f"{schema}._snapshots"

    new_snapshots = []

    for name, identifier, version, snapshopt in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot").from_(snapshots_table)
    ):
        snapshot = json.loads(snapshopt)
        pre_hooks = snapshot["model"].pop("pre", [])
        post_hooks = snapshot["model"].pop("post", [])

        expressions = snapshot["model"].pop("expressions", None)
        if expressions and snapshot["model"]["source_type"] == "sql":
            snapshot["model"]["pre_statements"] = expressions

        if pre_hooks or post_hooks:
            print(
                "WARNING: Hooks are no longer supported by SQLMesh, use pre and post SQL statements instead. "
                f"Removing 'pre' and 'post' attributes from snapshot name='{name}', identifier='{identifier}'"
            )

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(snapshot),
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
            },
            contains_json=True,
        )
