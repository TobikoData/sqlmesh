"""Remove superfluous exp.Paren references from partitioned_by"""

import json

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type
from sqlmesh.utils.migration import blob_text_type


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    pass


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
    import pandas as pd

    snapshots_table = "_snapshots"
    index_type = index_text_type(engine_adapter.dialect)
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    new_snapshots = []
    updated = False

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
        exp.select(
            "name",
            "identifier",
            "version",
            "snapshot",
            "kind_name",
            "updated_ts",
            "unpaused_ts",
            "ttl_ms",
            "unrestorable",
        ).from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)

        if partitioned_by := parsed_snapshot["node"].get("partitioned_by"):
            new_partitioned_by = []
            for item in partitioned_by:
                # rewrite '(foo)' to 'foo'
                if item.startswith("(") and item.endswith(")"):
                    item = item[1:-1]
                    updated = True
                new_partitioned_by.append(item)
            parsed_snapshot["node"]["partitioned_by"] = new_partitioned_by

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

    if new_snapshots and updated:
        engine_adapter.delete_from(snapshots_table, "TRUE")
        blob_type = blob_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            target_columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build(blob_type),
                "kind_name": exp.DataType.build(index_type),
                "updated_ts": exp.DataType.build("bigint"),
                "unpaused_ts": exp.DataType.build("bigint"),
                "ttl_ms": exp.DataType.build("bigint"),
                "unrestorable": exp.DataType.build("boolean"),
            },
        )
