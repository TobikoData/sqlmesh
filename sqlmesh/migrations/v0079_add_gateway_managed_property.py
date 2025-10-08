"""Add flag that controls whether the virtual layer's views will be created by the model specified gateway rather than the default gateway."""

from sqlglot import exp


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    environments_table = "_environments"
    if schema:
        environments_table = f"{schema}.{environments_table}"

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


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
    environments_table = "_environments"
    if schema:
        environments_table = f"{schema}.{environments_table}"

    engine_adapter.update_table(
        environments_table,
        {"gateway_managed": False},
        where=exp.true(),
    )
