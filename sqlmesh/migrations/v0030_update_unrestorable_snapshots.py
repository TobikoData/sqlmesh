"""Update unrestorable snapshots."""

import json
import typing as t
from collections import defaultdict

import pandas as pd
from sqlglot import exp

from sqlmesh.utils.migration import index_text_type


def migrate(state_sync: t.Any, **kwargs: t.Any) -> None:  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    new_snapshots = []
    snapshots_by_version = defaultdict(list)

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        snapshots_by_version[(name, version)].append((identifier, kind_name, parsed_snapshot))

    for (name, version), snapshots in snapshots_by_version.items():
        has_forward_only = any(s["change_category"] == 3 for _, _, s in snapshots)
        for identifier, kind_name, snapshot in snapshots:
            if (
                has_forward_only
                and snapshot["change_category"] != 3
                and not snapshot.get("unpaused_ts")
            ):
                snapshot["unrestorable"] = True
            new_snapshots.append(
                {
                    "name": name,
                    "identifier": identifier,
                    "version": version,
                    "snapshot": json.dumps(snapshot),
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
            contains_json=True,
        )
