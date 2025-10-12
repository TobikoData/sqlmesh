"""Add the environment statements table."""

from sqlglot import exp

from sqlmesh.utils.migration import blob_text_type, index_text_type


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    environment_statements_table = "_environment_statements"

    if schema:
        environment_statements_table = f"{schema}.{environment_statements_table}"

    index_type = index_text_type(engine_adapter.dialect)
    blob_type = blob_text_type(engine_adapter.dialect)

    engine_adapter.create_state_table(
        environment_statements_table,
        {
            "environment_name": exp.DataType.build(index_type),
            "plan_id": exp.DataType.build("text"),
            "environment_statements": exp.DataType.build(blob_type),
        },
        primary_key=("environment_name",),
    )


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
    pass
