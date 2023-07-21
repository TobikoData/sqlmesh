"""Include a set of snapshot IDs filtered for promotion."""
from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    environments_table = "_environments"
    if state_sync.schema:
        environments_table = f"{state_sync.schema}.{environments_table}"

    alter_table_exp = exp.AlterTable(
        this=exp.to_table(environments_table),
        actions=[
            exp.ColumnDef(
                this=exp.to_column("promoted_snapshot_ids"),
                kind=exp.DataType.build("text"),
            )
        ],
    )

    engine_adapter.execute(alter_table_exp)
