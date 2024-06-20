"""Add flag that controls whether environment names will be normalized."""

from sqlglot import exp


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    environments_table = "_environments"
    if state_sync.schema:
        environments_table = f"{state_sync.schema}.{environments_table}"

    alter_table_exp = exp.AlterTable(
        this=exp.to_table(environments_table),
        actions=[
            exp.ColumnDef(
                this=exp.to_column("normalize_name"),
                kind=exp.DataType.build("boolean"),
            )
        ],
    )
    engine_adapter.execute(alter_table_exp)

    state_sync.engine_adapter.update_table(
        environments_table,
        {"normalize_name": False},
        where=exp.true(),
    )
