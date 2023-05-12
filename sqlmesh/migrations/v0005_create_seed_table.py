"""Create a dedicated table to store the content of seeds."""
from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    seeds_table = f"{state_sync.schema}._seeds"

    engine_adapter.create_state_table(
        seeds_table,
        {
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "content": exp.DataType.build("text"),
        },
        primary_key=("name", "identifier"),
    )
