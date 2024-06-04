"""Rename the node attribute `column_descriptions_` to `column_descriptions` in snapshots."""

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
    found_col_descriptions = False

    for name, identifier, version, snapshot, kind_name, expiration_ts in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name", "expiration_ts").from_(
            snapshots_table
        ),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)

        if "column_descriptions_" in parsed_snapshot["node"]:
            found_col_descriptions = True
            parsed_snapshot["node"]["column_descriptions"] = parsed_snapshot["node"].pop(
                "column_descriptions_"
            )

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

    if found_col_descriptions:
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
