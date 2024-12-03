"""Remove dbt is_incremental macro"""

import json

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type
from sqlmesh.utils.migration import blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    blob_type = blob_text_type(engine_adapter.dialect)
    new_snapshots = []
    found_dbt_package = False
    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]
        dbt_package = node.get("jinja_macros", {}).get("packages", {}).get("dbt", {})

        if dbt_package:
            found_dbt_package = True
            dbt_package.pop("is_incremental", None)
            dbt_package.pop("should_full_refresh", None)

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
            }
        )

    if found_dbt_package:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build(blob_type),
                "kind_name": exp.DataType.build(index_type),
            },
        )
