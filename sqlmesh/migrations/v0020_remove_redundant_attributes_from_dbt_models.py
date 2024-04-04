"""Remove redundant attributes from dbt models."""

import json

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
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
        jinja_macros_global_objs = parsed_snapshot["node"]["jinja_macros"]["global_objs"]
        if "config" in jinja_macros_global_objs and isinstance(
            jinja_macros_global_objs["config"], dict
        ):
            for key in CONFIG_ATTRIBUTE_KEYS_TO_REMOVE:
                jinja_macros_global_objs["config"].pop(key, None)

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


CONFIG_ATTRIBUTE_KEYS_TO_REMOVE = [
    "config",
    "config_call_dict",
    "depends_on",
    "dependencies",
    "metrics",
    "original_file_path",
    "packages",
    "patch_path",
    "path",
    "post-hook",
    "pre-hook",
    "raw_code",
    "refs",
    "resource_type",
    "sources",
    "sql",
    "tests",
    "unrendered_config",
]
