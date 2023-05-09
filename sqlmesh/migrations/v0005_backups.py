from sqlglot import exp


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema

    backups_table = f"{schema}._backups"

    engine_adapter.create_state_table(
        backups_table,
        {
            "schema_version": exp.DataType.build("int"),
            "sqlglot_version": exp.DataType.build("text"),
            "table_name": exp.DataType.build("text"),
            "backup_table_name": exp.DataType.build("text"),
        },
        primary_key=("schema_version", "table_name"),
    )
