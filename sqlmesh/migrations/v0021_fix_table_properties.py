"""Fix table properties that have extra quoting due to a bug."""

import json

import pandas as pd
from sqlglot import exp

from sqlmesh.core import dialect as d
from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    new_snapshots = []
    found_table_properties = False
    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        table_properties = parsed_snapshot["node"].get("table_properties")
        if table_properties:
            found_table_properties = True
            dialect = parsed_snapshot["node"].get("dialect")
            parsed_snapshot["node"]["table_properties"] = exp.Tuple(
                expressions=[
                    exp.Literal.string(k).eq(d.parse_one(v)) for k, v in table_properties.items()
                ]
            ).sql(dialect=dialect)

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
            }
        )

    if found_table_properties:
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
