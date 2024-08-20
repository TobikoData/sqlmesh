"""Add the expiration_ts column to the snapshots table."""

import json

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.date import to_datetime, to_timestamp
from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    index_type = index_text_type(engine_adapter.dialect)

    alter_table_exp = exp.Alter(
        this=exp.to_table(snapshots_table),
        kind="TABLE",
        actions=[
            exp.ColumnDef(
                this=exp.to_column("expiration_ts"),
                kind=exp.DataType.build("bigint"),
            )
        ],
    )
    engine_adapter.execute(alter_table_exp)

    new_snapshots = []

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)

        updated_ts = parsed_snapshot["updated_ts"]
        ttl = parsed_snapshot["ttl"]
        expiration_ts = to_timestamp(ttl, relative_base=to_datetime(updated_ts))

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": snapshot,
                "kind_name": kind_name,
                "expiration_ts": expiration_ts,
            }
        )

    if new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

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
