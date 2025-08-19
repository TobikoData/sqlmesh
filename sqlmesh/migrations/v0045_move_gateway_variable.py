"""Move the gateway variable."""

import ast
import json

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type
from sqlmesh.utils.migration import blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    import pandas as pd

    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    migration_needed = False
    new_snapshots = []

    for name, identifier, version, snapshot, kind_name, expiration_ts in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name", "expiration_ts").from_(
            snapshots_table
        ),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        python_env = parsed_snapshot["node"].get("python_env")
        if python_env:
            gateway = python_env.pop("gateway", None)
            if gateway is not None:
                migration_needed = True
                sqlmesh_vars = {"gateway": ast.literal_eval(gateway["payload"])}
                python_env["__sqlmesh__vars__"] = {
                    "payload": repr(sqlmesh_vars),
                    "kind": "value",
                }

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
                "expiration_ts": expiration_ts,
            }
        )

    if migration_needed and new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)
        blob_type = blob_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            target_columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build(blob_type),
                "kind_name": exp.DataType.build(index_type),
                "expiration_ts": exp.DataType.build("bigint"),
            },
        )
