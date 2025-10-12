"""Add dev version to the intervals table."""

from sqlglot import exp


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    intervals_table = "_intervals"
    if schema:
        intervals_table = f"{schema}.{intervals_table}"

    alter_table_exp = exp.Alter(
        this=exp.to_table(intervals_table),
        kind="TABLE",
        actions=[
            exp.ColumnDef(
                this=exp.to_column("last_altered_ts"),
                kind=exp.DataType.build("BIGINT", dialect=engine_adapter.dialect),
            )
        ],
    )
    engine_adapter.execute(alter_table_exp)


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
    pass
