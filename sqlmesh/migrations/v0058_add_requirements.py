"""Add requirements to environments table"""

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
                this=exp.to_column("requirements"),
                kind=exp.DataType.build("text"),
            )
        ],
    )

    engine_adapter.execute(alter_table_exp)
