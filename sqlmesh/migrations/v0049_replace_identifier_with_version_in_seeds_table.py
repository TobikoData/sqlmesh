"""Use version instead of identifier in the seeds table."""

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter

    snapshots_table = "_snapshots"
    seeds_table = "_seeds"
    new_seeds_table = f"{seeds_table}_v49"

    if state_sync.schema:
        snapshots_table = f"{state_sync.schema}.{snapshots_table}"
        seeds_table = f"{state_sync.schema}.{seeds_table}"
        new_seeds_table = f"{state_sync.schema}.{new_seeds_table}"

    index_type = index_text_type(engine_adapter.dialect)

    engine_adapter.drop_table(new_seeds_table)
    engine_adapter.create_state_table(
        new_seeds_table,
        {
            "name": exp.DataType.build(index_type),
            "version": exp.DataType.build(index_type),
            "content": exp.DataType.build("text"),
        },
        primary_key=("name", "version"),
    )

    name_col = exp.column("name", table="seeds")
    version_col = exp.column("version", table="snapshots")
    query = (
        exp.select(
            name_col,
            version_col,
            exp.func("MAX", exp.column("content", table="seeds")).as_("content"),
        )
        .from_(exp.to_table(seeds_table).as_("seeds"))
        .join(
            exp.to_table(snapshots_table).as_("snapshots"),
            on=exp.and_(
                exp.column("name", table="seeds").eq(exp.column("name", table="snapshots")),
                exp.column("identifier", table="seeds").eq(
                    exp.column("identifier", table="snapshots")
                ),
            ),
        )
        .where(exp.column("version", table="snapshots").is_(exp.null()).not_())
        .group_by(name_col, version_col)
    )

    engine_adapter.insert_append(new_seeds_table, query)
    engine_adapter.drop_table(seeds_table)
    engine_adapter.rename_table(new_seeds_table, seeds_table)
