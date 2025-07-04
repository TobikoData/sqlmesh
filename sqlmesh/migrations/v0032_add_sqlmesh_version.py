"""Add new 'sqlmesh_version' column to the version state table."""

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    state_tables = getattr(state_sync, "state_tables", {})
    versions_table = state_tables.get("versions_table", "_versions")
    if state_sync.schema:
        versions_table = f"{state_sync.schema}.{versions_table}"
    index_type = index_text_type(engine_adapter.dialect)
    alter_table_exp = exp.Alter(
        this=exp.to_table(versions_table),
        kind="TABLE",
        actions=[
            exp.ColumnDef(
                this=exp.to_column("sqlmesh_version"),
                kind=exp.DataType.build(index_type),
            )
        ],
    )

    engine_adapter.execute(alter_table_exp)
