"""Add updated_ts, unpaused_ts, ttl_ms, and unrestorable columns to the snapshots table."""

import json

from sqlglot import exp

from sqlmesh.utils.date import to_datetime, to_timestamp
from sqlmesh.utils.migration import index_text_type
from sqlmesh.utils.migration import blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    import pandas as pd

    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    index_type = index_text_type(engine_adapter.dialect)
    blob_type = blob_text_type(engine_adapter.dialect)

    add_column_exps = [
        exp.Alter(
            this=exp.to_table(snapshots_table),
            kind="TABLE",
            actions=[
                exp.ColumnDef(
                    this=exp.to_column(column_name),
                    kind=exp.DataType.build("bigint"),
                )
            ],
        )
        for column_name in ["updated_ts", "unpaused_ts", "ttl_ms"]
    ] + [
        exp.Alter(
            this=exp.to_table(snapshots_table),
            kind="TABLE",
            actions=[
                exp.ColumnDef(
                    this=exp.to_column("unrestorable"),
                    kind=exp.DataType.build("boolean"),
                )
            ],
        )
    ]
    engine_adapter.execute(add_column_exps)

    if engine_adapter.dialect == "databricks":
        # Databricks will throw an error like:
        # > databricks.sql.exc.ServerOperationError: [DELTA_UNSUPPORTED_DROP_COLUMN] DROP COLUMN is not supported for your Delta table.
        # when we try to drop `expiration_ts` below unless we set delta.columnMapping.mode to 'name'
        alter_table_exp = exp.Alter(
            this=exp.to_table(snapshots_table),
            kind="TABLE",
            actions=[
                exp.AlterSet(
                    expressions=[
                        exp.Properties(
                            expressions=[
                                exp.Property(
                                    this=exp.Literal.string("delta.columnMapping.mode"),
                                    value=exp.Literal.string("name"),
                                )
                            ]
                        )
                    ]
                )
            ],
        )
        engine_adapter.execute(alter_table_exp)

    drop_column_exp = exp.Alter(
        this=exp.to_table(snapshots_table),
        kind="TABLE",
        actions=[exp.Drop(this=exp.to_column("expiration_ts"), kind="COLUMN")],
    )
    engine_adapter.execute(drop_column_exp)

    new_snapshots = []

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        updated_ts = parsed_snapshot.pop("updated_ts")
        unpaused_ts = parsed_snapshot.pop("unpaused_ts", None)
        ttl_ms = max(
            to_timestamp(
                parsed_snapshot["ttl"],
                relative_base=to_datetime(updated_ts),
                check_categorical_relative_expression=False,
            )
            - updated_ts,
            0,
        )
        unrestorable = parsed_snapshot.pop("unrestorable", False)

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
                "updated_ts": updated_ts,
                "unpaused_ts": unpaused_ts,
                "ttl_ms": ttl_ms,
                "unrestorable": unrestorable,
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
                "snapshot": exp.DataType.build(blob_type),
                "kind_name": exp.DataType.build(index_type),
                "updated_ts": exp.DataType.build("bigint"),
                "unpaused_ts": exp.DataType.build("bigint"),
                "ttl_ms": exp.DataType.build("bigint"),
                "unrestorable": exp.DataType.build("boolean"),
            },
        )
