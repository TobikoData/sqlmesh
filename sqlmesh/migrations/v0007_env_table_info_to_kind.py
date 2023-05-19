"""Create a dedicated table to store the content of seeds."""
import json
import zlib

import pandas as pd
from sqlglot import exp


def _hash(data):
    return str(zlib.crc32(";".join("" if d is None else d for d in data).encode("utf-8")))


def migrate(state_sync):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    environments_table = f"{schema}._environments"
    snapshots_to_kind = {}

    for name, identifier, snapshot in engine_adapter.fetchall(f"SELECT name, identifier, snapshot FROM {schema}._snapshots"):
        snapshot = json.loads(snapshot)
        snapshots_to_kind[(name, identifier)] = snapshot["model"]["kind"]["name"]

    environments = engine_adapter.fetchall(f"SELECT * FROM {environments_table}")
    new_environments = []

    for name, snapshots, start_at, end_at, plan_id, previous_plan_id, expiration_ts, finalized_ts in environments:
        new_snapshots = []

        for snapshot in json.loads(snapshots):
            snapshot.pop("is_materialized", None)
            snapshot.pop("is_embedded_kind", None)

            fingerprint = snapshot["fingerprint"]
            identifier = _hash(
                [
                    fingerprint["data_hash"],
                    fingerprint["metadata_hash"],
                    fingerprint["parent_data_hash"],
                    fingerprint["parent_metadata_hash"],
                ]
            )

            snapshot["kind_name"] = snapshots_to_kind[(snapshot["name"], identifier)]
            new_snapshots.append(snapshot)

        new_environments.append(
            {
                "name": name,
                "snapshots": json.dumps(new_snapshots),
                "start_at": start_at,
                "end_at": end_at,
                "plan_id": plan_id,
                "previous_plan_id": previous_plan_id,
                "expiration_ts": expiration_ts,
                "finalized_ts": finalized_ts,
            }
        )

    if new_environments:
        engine_adapter.delete_from(environments_table, "TRUE")

        engine_adapter.insert_append(
            environments_table,
            pd.DataFrame(new_environments),
            columns_to_types={
                "name": exp.DataType.build("text"),
                "snapshots": exp.DataType.build("text"),
                "start_at": exp.DataType.build("text"),
                "end_at": exp.DataType.build("text"),
                "plan_id": exp.DataType.build("text"),
                "previous_plan_id": exp.DataType.build("text"),
                "expiration_ts": exp.DataType.build("bigint"),
                "finalized_ts": exp.DataType.build("bigint"),
            },
            contains_json=True,
        )
