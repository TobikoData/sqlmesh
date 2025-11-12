"""Duplicate of v0033, Use LONGTEXT type for blob fields in MySQL.

Seeds table has since been dropped.
Environments table now has a requirements column.
"""

from sqlglot import exp

from sqlmesh.utils.migration import blob_text_type


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    if engine_adapter.dialect != "mysql":
        return
    environments_table = "_environments"
    snapshots_table = "_snapshots"

    if schema:
        environments_table = f"{schema}.{environments_table}"
        snapshots_table = f"{schema}.{snapshots_table}"

    targets = [
        (environments_table, "snapshots"),
        (environments_table, "promoted_snapshot_ids"),
        (environments_table, "previous_finalized_snapshots"),
        (environments_table, "requirements"),
        (snapshots_table, "snapshot"),
    ]

    for table_name, column_name in targets:
        blob_type = blob_text_type(engine_adapter.dialect)
        alter_table_exp = exp.Alter(
            this=exp.to_table(table_name),
            kind="TABLE",
            actions=[
                exp.AlterColumn(
                    this=exp.to_column(column_name),
                    dtype=exp.DataType.build(blob_type),
                )
            ],
        )

        engine_adapter.execute(alter_table_exp)


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
    pass
