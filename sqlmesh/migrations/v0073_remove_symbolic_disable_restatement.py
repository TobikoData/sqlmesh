"""Remove disable restatement from external and embedded models."""

import json
import pandas as pd

from sqlglot import exp
from sqlmesh.utils.migration import index_text_type, blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    index_type = index_text_type(engine_adapter.dialect)
    blob_type = blob_text_type(engine_adapter.dialect)
    snapshots_columns_to_types = {
        "name": exp.DataType.build(index_type),
        "identifier": exp.DataType.build(index_type),
        "version": exp.DataType.build(index_type),
        "snapshot": exp.DataType.build(blob_type),
        "kind_name": exp.DataType.build(index_type),
        "updated_ts": exp.DataType.build("bigint"),
        "unpaused_ts": exp.DataType.build("bigint"),
        "ttl_ms": exp.DataType.build("bigint"),
        "unrestorable": exp.DataType.build("boolean"),
    }

    new_snapshots = []
    for (
        name,
        identifier,
        version,
        snapshot,
        kind_name,
        updated_ts,
        unpaused_ts,
        ttl_ms,
        unrestorable,
    ) in engine_adapter.fetchall(
        exp.select(*snapshots_columns_to_types).from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        kind = parsed_snapshot["node"].get("kind")

        if kind and kind_name in ("EMBEDDED", "EXTERNAL"):
            kind.pop("disable_restatement", None)

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
                "updated_ts": updated_ts,
                "unpaused_ts": unpaused_ts,
                "ttl_ms": ttl_ms,
                "unrestorable": unrestorable,
            }
        )

    if new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")
        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            columns_to_types=snapshots_columns_to_types,
        )
