"""Create a dedicated table to store the content of seeds."""
from sqlglot import exp

from sqlmesh.utils.migration import primary_key_text_type


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    seeds_table = "_seeds"
    if state_sync.schema:
        seeds_table = f"{state_sync.schema}.{seeds_table}"

    pk_text_type = primary_key_text_type(engine_adapter.dialect)

    engine_adapter.create_state_table(
        seeds_table,
        {
            "name": exp.DataType.build(pk_text_type),
            "identifier": exp.DataType.build(pk_text_type),
            "content": exp.DataType.build("text"),
        },
        primary_key=("name", "identifier"),
    )
