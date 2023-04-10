"""All migrations should be named _XXXX.py, they will be executed sequentially.

If a migration alters the payload of any pydantic models, you should not actually use them because
the running model may not be able to load them. Make sure that these migration files are standalone.
"""
from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    engine_adapter.create_schema(schema)

    snapshots_table = f"{schema}._snapshots"
    environments_table = f"{schema}._environments"
    versions_table = f"{schema}._versions"

    engine_adapter.create_state_table(
        snapshots_table,
        {
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "snapshot": exp.DataType.build("text"),
        },
        primary_key=("name", "identifier"),
    )

    engine_adapter.create_index(snapshots_table, "name_version_idx", ("name", "version"))

    engine_adapter.create_state_table(
        environments_table,
        {
            "name": exp.DataType.build("text"),
            "snapshots": exp.DataType.build("text"),
            "start_at": exp.DataType.build("text"),
            "end_at": exp.DataType.build("text"),
            "plan_id": exp.DataType.build("text"),
            "previous_plan_id": exp.DataType.build("text"),
            "expiration_ts": exp.DataType.build("bigint"),
        },
        primary_key=("name",),
    )

    engine_adapter.create_state_table(
        versions_table,
        {
            "schema_version": exp.DataType.build("int"),
            "sqlglot_version": exp.DataType.build("text"),
        },
    )
