"""Add support for environment finalization."""
from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    environments_table = state_sync.environments_table

    alter_table_exp = exp.AlterTable(
        this=exp.to_table(environments_table),
        actions=[
            exp.ColumnDef(
                this=exp.to_column("finalized_at_ts"),
                kind=exp.DataType.build("bigint"),
            )
        ],
    )

    engine_adapter.execute(alter_table_exp)
