"""Add flag that controls whether the virtual layer's views will be created by the model specified gateway rather than the default gateway."""

from sqlglot import exp


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    environments_table = "_environments"
    if state_sync.schema:
        environments_table = f"{state_sync.schema}.{environments_table}"

    alter_table_exp = exp.Alter(
        this=exp.to_table(environments_table),
        kind="TABLE",
        actions=[
            exp.ColumnDef(
                this=exp.to_column("gateway_managed"),
                kind=exp.DataType.build("boolean"),
            )
        ],
    )
    engine_adapter.execute(alter_table_exp)

    state_sync.engine_adapter.update_table(
        environments_table,
        {"gateway_managed": False},
        where=exp.true(),
    )
