"""Create a dedicated table to store snapshot intervals."""
from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    intervals_table = f"{state_sync.schema}._intervals"

    engine_adapter.create_state_table(
        intervals_table,
        {
            "id": exp.DataType.build("text"),
            "created_ts": exp.DataType.build("bigint"),
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "start_ts": exp.DataType.build("bigint"),
            "end_ts": exp.DataType.build("bigint"),
            "is_dev": exp.DataType.build("boolean"),
            "is_removed": exp.DataType.build("boolean"),
            "is_compacted": exp.DataType.build("boolean"),
        },
        primary_key=("id",),
    )

    engine_adapter.create_index(
        intervals_table, "name_version_idx", ("name", "version", "created_ts")
    )
    engine_adapter.create_index(
        intervals_table, "name_identifier_idx", ("name", "identifier", "created_ts")
    )
