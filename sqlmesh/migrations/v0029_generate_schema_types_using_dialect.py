"""Generate mapping schema data types using the corresponding model's dialect."""

import json

from sqlglot import exp, parse_one

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    import pandas as pd

    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    new_snapshots = []
    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]

        mapping_schema = node.get("mapping_schema")
        if mapping_schema:
            node["mapping_schema"] = _convert_schema_types(mapping_schema, node["dialect"])

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
            }
        )

    if new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build("text"),
                "kind_name": exp.DataType.build(index_type),
            },
        )


def _convert_schema_types(schema, dialect):  # type: ignore
    if not schema:
        return schema

    for k, v in schema.items():
        if isinstance(v, dict):
            _convert_schema_types(v, dialect)
        else:
            schema[k] = parse_one(v).sql(dialect=dialect)

    return schema
