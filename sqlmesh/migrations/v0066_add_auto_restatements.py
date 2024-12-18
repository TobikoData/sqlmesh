"""Add the auto restatements table."""

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    auto_restatements_table = "_auto_restatements"
    intervals_table = "_intervals"

    if schema:
        auto_restatements_table = f"{schema}.{auto_restatements_table}"
        intervals_table = f"{schema}.{intervals_table}"

    index_type = index_text_type(engine_adapter.dialect)

    engine_adapter.create_state_table(
        auto_restatements_table,
        {
            "snapshot_name": exp.DataType.build(index_type),
            "snapshot_version": exp.DataType.build(index_type),
            "next_auto_restatement_ts": exp.DataType.build("bigint"),
        },
        primary_key=("snapshot_name", "snapshot_version"),
    )

    alter_table_exp = exp.Alter(
        this=exp.to_table(intervals_table),
        kind="TABLE",
        actions=[
            exp.ColumnDef(
                this=exp.to_column("is_pending_restatement"),
                kind=exp.DataType.build("boolean"),
            )
        ],
    )
    engine_adapter.execute(alter_table_exp)

    engine_adapter.update_table(
        intervals_table,
        {"is_pending_restatement": False},
        where=exp.true(),
    )
