"""Add new 'pre_check_version' column to the version state table."""

from sqlglot import exp


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    versions_table = "_versions"
    if schema:
        versions_table = f"{schema}.{versions_table}"

    alter_table_exp = exp.Alter(
        this=exp.to_table(versions_table),
        kind="TABLE",
        actions=[
            exp.ColumnDef(
                this=exp.to_column("pre_check_version"),
                kind=exp.DataType.build("int"),
            )
        ],
    )

    engine_adapter.execute(alter_table_exp)
