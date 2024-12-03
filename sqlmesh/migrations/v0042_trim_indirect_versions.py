"""Trim irrelevant attributes from indirect versions."""

import json

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type
from sqlmesh.utils.migration import blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    new_snapshots = []

    for name, identifier, version, snapshot, kind_name, expiration_ts in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name", "expiration_ts").from_(
            snapshots_table
        ),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        for indirect_versions in parsed_snapshot["indirect_versions"].values():
            for indirect_version in indirect_versions:
                # Only keep version and change_category.
                version = indirect_version.get("version")
                change_category = indirect_version.get("change_category")
                indirect_version.clear()
                indirect_version["version"] = version
                indirect_version["change_category"] = change_category

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
                "expiration_ts": expiration_ts,
            }
        )

    if new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)
        blob_type = blob_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build(blob_type),
                "kind_name": exp.DataType.build(index_type),
                "expiration_ts": exp.DataType.build("bigint"),
            },
        )
