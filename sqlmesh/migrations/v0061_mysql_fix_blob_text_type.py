"""Duplicate of v0033, Use LONGTEXT type for blob fields in MySQL.

Seeds table has since been dropped.
Environments table now has a requirements column.
"""

from sqlglot import exp

from sqlmesh.utils.migration import blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    if engine_adapter.dialect != "mysql":
        return

    schema = state_sync.schema
    environments_table = "_environments"
    snapshots_table = "_snapshots"
    plan_dags_table = "_plan_dags"

    if schema:
        environments_table = f"{schema}.{environments_table}"
        snapshots_table = f"{schema}.{snapshots_table}"
        plan_dags_table = f"{schema}.{plan_dags_table}"

    targets = [
        (environments_table, "snapshots"),
        (environments_table, "promoted_snapshot_ids"),
        (environments_table, "previous_finalized_snapshots"),
        (environments_table, "requirements"),
        (snapshots_table, "snapshot"),
        (plan_dags_table, "dag_spec"),
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
