"""Update the dev table suffix to be 'dev'. Rename temp_version to dev_version."""

import json

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type, blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    import pandas as pd

    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    environments_table = "_environments"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"
        environments_table = f"{schema}.{environments_table}"

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
    environments_columns_to_types = {
        "name": exp.DataType.build(index_type),
        "snapshots": exp.DataType.build(blob_type),
        "start_at": exp.DataType.build("text"),
        "end_at": exp.DataType.build("text"),
        "plan_id": exp.DataType.build("text"),
        "previous_plan_id": exp.DataType.build("text"),
        "expiration_ts": exp.DataType.build("bigint"),
        "finalized_ts": exp.DataType.build("bigint"),
        "promoted_snapshot_ids": exp.DataType.build(blob_type),
        "suffix_target": exp.DataType.build("text"),
        "catalog_name_override": exp.DataType.build("text"),
        "previous_finalized_snapshots": exp.DataType.build(blob_type),
        "normalize_name": exp.DataType.build("boolean"),
        "requirements": exp.DataType.build(blob_type),
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
        parsed_snapshot = _update_snapshot(parsed_snapshot)

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
            target_columns_to_types=snapshots_columns_to_types,
        )

    new_environments = []
    for (
        name,
        snapshots,
        start_at,
        end_at,
        plan_id,
        previous_plan_id,
        expiration_ts,
        finalized_ts,
        promoted_snapshot_ids,
        suffix_target,
        catalog_name_override,
        previous_finalized_snapshots,
        normalize_name,
        requirements,
    ) in engine_adapter.fetchall(
        exp.select(*environments_columns_to_types).from_(environments_table),
        quote_identifiers=True,
    ):
        if snapshots:
            parsed_snapshots = json.loads(snapshots)
            for s in parsed_snapshots:
                _update_snapshot(s)

        if previous_finalized_snapshots:
            parsed_previous_finalized_snapshots = json.loads(previous_finalized_snapshots)
            for s in parsed_previous_finalized_snapshots:
                _update_snapshot(s)

        new_environments.append(
            {
                "name": name,
                "snapshots": json.dumps(parsed_snapshots) if snapshots else None,
                "start_at": start_at,
                "end_at": end_at,
                "plan_id": plan_id,
                "previous_plan_id": previous_plan_id,
                "expiration_ts": expiration_ts,
                "finalized_ts": finalized_ts,
                "promoted_snapshot_ids": promoted_snapshot_ids,
                "suffix_target": suffix_target,
                "catalog_name_override": catalog_name_override,
                "previous_finalized_snapshots": json.dumps(parsed_previous_finalized_snapshots)
                if previous_finalized_snapshots
                else None,
                "normalize_name": normalize_name,
                "requirements": requirements,
            }
        )

    if new_environments:
        engine_adapter.delete_from(environments_table, "TRUE")
        engine_adapter.insert_append(
            environments_table,
            pd.DataFrame(new_environments),
            target_columns_to_types=environments_columns_to_types,
        )


def _update_snapshot(snapshot: dict) -> dict:
    snapshot = _update_fields(snapshot)

    if "previous_versions" in snapshot:
        for previous_version in snapshot["previous_versions"]:
            _update_fields(previous_version)

    return snapshot


def _update_fields(target: dict) -> dict:
    # Setting the old suffix to match the names of existing tables.
    target["dev_table_suffix"] = "temp"
    if "temp_version" in target:
        target["dev_version"] = target.pop("temp_version")
    return target
