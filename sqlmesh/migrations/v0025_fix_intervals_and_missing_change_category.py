"""Normalize intervals and fix missing change category."""

import json
import zlib

import pandas as pd
from sqlglot import exp

from sqlmesh.utils import random_id
from sqlmesh.utils.date import now_timestamp
from sqlmesh.utils.migration import index_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    intervals_table = "_intervals"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"
        intervals_table = f"{schema}.{intervals_table}"

    migration_required = False
    new_snapshots = []
    new_intervals = []

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)

        if not parsed_snapshot.get("change_category"):
            fingerprint = parsed_snapshot.get("fingerprint")
            version = _hash(
                [
                    fingerprint["data_hash"],
                    fingerprint["parent_data_hash"],
                ]
            )
            parsed_snapshot["change_category"] = (
                4 if version == parsed_snapshot.get("version") else 5
            )
            migration_required = True

        def _add_interval(start_ts: int, end_ts: int, is_dev: bool) -> None:
            new_intervals.append(
                {
                    "id": random_id(),
                    "created_ts": now_timestamp(),
                    "name": name,
                    "identifier": identifier,
                    "version": version,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "is_dev": is_dev,
                    "is_removed": False,
                    "is_compacted": True,
                }
            )

        for interval in parsed_snapshot.pop("intervals", []):
            _add_interval(interval[0], interval[1], False)
            migration_required = True

        for interval in parsed_snapshot.pop("dev_intervals", []):
            _add_interval(interval[0], interval[1], True)
            migration_required = True

        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
            }
        )

    if migration_required:
        index_type = index_text_type(engine_adapter.dialect)

        engine_adapter.delete_from(snapshots_table, "TRUE")
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

        if new_intervals:
            engine_adapter.insert_append(
                intervals_table,
                pd.DataFrame(new_intervals),
                columns_to_types={
                    "id": exp.DataType.build(index_type),
                    "created_ts": exp.DataType.build("bigint"),
                    "name": exp.DataType.build(index_type),
                    "identifier": exp.DataType.build(index_type),
                    "version": exp.DataType.build(index_type),
                    "start_ts": exp.DataType.build("bigint"),
                    "end_ts": exp.DataType.build("bigint"),
                    "is_dev": exp.DataType.build("boolean"),
                    "is_removed": exp.DataType.build("boolean"),
                    "is_compacted": exp.DataType.build("boolean"),
                },
            )


def _hash(data):  # type: ignore
    return str(zlib.crc32(";".join("" if d is None else d for d in data).encode("utf-8")))
