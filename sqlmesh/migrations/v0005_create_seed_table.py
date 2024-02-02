"""Create a dedicated table to store the content of seeds."""

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    seeds_table = "_seeds"
    if state_sync.schema:
        seeds_table = f"{state_sync.schema}.{seeds_table}"

    index_type = index_text_type(engine_adapter.dialect)

    engine_adapter.create_state_table(
        seeds_table,
        {
            "name": exp.DataType.build(index_type),
            "identifier": exp.DataType.build(index_type),
            "content": exp.DataType.build("text"),
        },
        primary_key=("name", "identifier"),
    )
