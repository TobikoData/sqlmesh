"""Fix the project dialect in snapshots for dbt projects."""

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

    found_missing_dialect = False
    new_snapshots = []

    for name, identifier, version, snapshot, kind_name, expiration_ts in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name", "expiration_ts").from_(
            snapshots_table
        ),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        jinja_macros_global_objs = parsed_snapshot["node"]["jinja_macros"]["global_objs"]
        if "dialect" not in jinja_macros_global_objs and jinja_macros_global_objs.get(
            "target", {}
        ).get("type"):
            jinja_macros_global_objs["dialect"] = jinja_macros_global_objs["target"]["type"]
            found_missing_dialect = True

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

    if new_snapshots and found_missing_dialect:
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
                "expiration_ts": exp.DataType.build("bigint"),
            },
        )
