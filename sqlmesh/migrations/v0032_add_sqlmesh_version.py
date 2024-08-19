"""Add new 'sqlmesh_version' column to the version state table."""

from sqlglot import exp


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    versions_table = "_versions"
    if state_sync.schema:
        versions_table = f"{state_sync.schema}.{versions_table}"

    alter_table_exp = exp.Alter(
        this=exp.to_table(versions_table),
        kind="TABLE",
        actions=[
            exp.ColumnDef(
                this=exp.to_column("sqlmesh_version"),
                kind=exp.DataType.build("text"),
            )
        ],
    )

    engine_adapter.execute(alter_table_exp)
