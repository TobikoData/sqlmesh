"""Add support for environment suffix target."""
import pandas as pd
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    environments_table = "_environments"
    if state_sync.schema:
        environments_table = f"{state_sync.schema}.{environments_table}"

    alter_table_exp = exp.AlterTable(
        this=exp.to_table(environments_table),
        actions=[
            exp.ColumnDef(
                this=exp.to_column("suffix_target"),
                kind=exp.DataType.build("text"),
            )
        ],
    )
    engine_adapter.execute(alter_table_exp)

    environments = []
    columns = [
        "name",
        "snapshots",
        "start_at",
        "end_at",
        "plan_id",
        "previous_plan_id",
        "expiration_ts",
        "finalized_ts",
        "promoted_snapshot_ids",
    ]
    for row in engine_adapter.fetchall(
        exp.select(*columns).from_(environments_table), quote_identifiers=True
    ):
        environments.append({**dict(zip(columns, row)), "suffix_target": "schema"})
    if environments:
        engine_adapter.delete_from(environments_table, "TRUE")

        text_type = index_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            environments_table,
            pd.DataFrame(environments),
            columns_to_types={
                "name": exp.DataType.build(text_type),
                "snapshots": exp.DataType.build(text_type),
                "start_at": exp.DataType.build(text_type),
                "end_at": exp.DataType.build(text_type),
                "plan_id": exp.DataType.build(text_type),
                "previous_plan_id": exp.DataType.build(text_type),
                "expiration_ts": exp.DataType.build("bigint"),
                "finalized_ts": exp.DataType.build("bigint"),
                "promoted_snapshot_ids": exp.DataType.build(text_type),
                "suffix_target": exp.DataType.build(text_type),
            },
        )
