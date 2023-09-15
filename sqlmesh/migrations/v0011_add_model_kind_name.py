"""Add the kind_name column to the snapshots table."""
import json

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    text_type = index_text_type(engine_adapter.dialect)

    alter_table_exp = exp.AlterTable(
        this=exp.to_table(snapshots_table),
        actions=[
            exp.ColumnDef(
                this=exp.to_column("kind_name"),
                kind=exp.DataType.build(text_type),
            )
        ],
    )
    engine_adapter.execute(alter_table_exp)

    new_snapshots = []

    for name, identifier, version, snapshot in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": snapshot,
                "kind_name": parsed_snapshot["model"]["kind"]["name"],
            }
        )

    if new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types={
                "name": exp.DataType.build(text_type),
                "identifier": exp.DataType.build(text_type),
                "version": exp.DataType.build(text_type),
                "snapshot": exp.DataType.build("text"),
                "kind_name": exp.DataType.build(text_type),
            },
            contains_json=True,
        )
