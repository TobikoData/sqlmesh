"""Add requirements to environments table"""

from sqlglot import exp

from sqlmesh.utils.migration import blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    environments_table = "_environments"
    if state_sync.schema:
        environments_table = f"{state_sync.schema}.{environments_table}"

    blob_type = blob_text_type(engine_adapter.dialect)
    alter_table_exp = exp.Alter(
        this=exp.to_table(environments_table),
        kind="TABLE",
        actions=[
            exp.ColumnDef(
                this=exp.to_column("requirements"),
                kind=exp.DataType.build(blob_type),
            )
        ],
    )

    engine_adapter.execute(alter_table_exp)
