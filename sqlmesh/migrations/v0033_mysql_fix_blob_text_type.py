"""Use LONGTEXT type for blob fields in MySQL."""

from sqlglot import exp


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    if engine_adapter.dialect != "mysql":
        return

    schema = state_sync.schema
    environments_table = "_environments"
    snapshots_table = "_snapshots"
    seeds_table = "_seeds"
    plan_dags_table = "_plan_dags"

    if schema:
        environments_table = f"{schema}.{environments_table}"
        snapshots_table = f"{schema}.{snapshots_table}"
        seeds_table = f"{state_sync.schema}.{seeds_table}"
        plan_dags_table = f"{schema}.{plan_dags_table}"

    targets = [
        (environments_table, "snapshots"),
        (snapshots_table, "snapshot"),
        (seeds_table, "content"),
        (plan_dags_table, "dag_spec"),
    ]

    for table_name, column_name in targets:
        alter_table_exp = exp.AlterTable(
            this=exp.to_table(table_name),
            actions=[
                exp.AlterColumn(
                    this=exp.to_column(column_name),
                    dtype=exp.DataType.build("longtext"),
                )
            ],
        )

        engine_adapter.execute(alter_table_exp)
