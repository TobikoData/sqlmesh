"""Remove dbt target fields from snapshots outside of limited list of approved fields"""

import json

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    new_snapshots = []
    found_dbt_target = False
    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]
        dbt_target = node.get("jinja_macros", {}).get("global_objs", {}).get("target", {})
        # Double check that `target_name` exists as a field since we know that all dbt targets have `target_name`
        # We do this in case someone has a target macro defined that is not related to dbt
        if dbt_target and dbt_target.get("target_name"):
            found_dbt_target = True
            node["jinja_macros"]["global_objs"]["target"] = {
                "type": dbt_target.get("type", "None"),
                "name": dbt_target.get("name", "None"),
                "schema": dbt_target.get("schema", "None"),
                "database": dbt_target.get("database", "None"),
                "target_name": dbt_target["target_name"],
            }

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
            }
        )

    if found_dbt_target:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build("text"),
                "kind_name": exp.DataType.build(index_type),
            },
        )
