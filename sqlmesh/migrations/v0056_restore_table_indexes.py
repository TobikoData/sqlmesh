"""Readds indexes and primary keys in case tables were restored from a backup."""

from sqlglot import exp
from sqlmesh.utils import random_id
from sqlmesh.utils.migration import index_text_type
from sqlmesh.utils.migration import blob_text_type


def migrate(state_sync, **kwargs):  # type: ignore
    schema = state_sync.schema
    engine_adapter = state_sync.engine_adapter
    if not engine_adapter.SUPPORTS_INDEXES:
        return

    intervals_table = "_intervals"
    snapshots_table = "_snapshots"
    environments_table = "_environments"
    if state_sync.schema:
        intervals_table = f"{schema}.{intervals_table}"
        snapshots_table = f"{schema}.{snapshots_table}"
        environments_table = f"{schema}.{environments_table}"

    table_suffix = random_id(short=True)

    index_type = index_text_type(engine_adapter.dialect)
    blob_type = blob_text_type(engine_adapter.dialect)

    new_snapshots_table = f"{snapshots_table}__{table_suffix}"
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

    new_environments_table = f"{environments_table}__{table_suffix}"
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
    }

    new_intervals_table = f"{intervals_table}__{table_suffix}"
    intervals_columns_to_types = {
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
    }

    # Recreate the snapshots table and its indexes.
    engine_adapter.create_table(
        new_snapshots_table, snapshots_columns_to_types, primary_key=("name", "identifier")
    )
    engine_adapter.create_index(
        new_snapshots_table, "_snapshots_name_version_idx", ("name", "version")
    )
    engine_adapter.insert_append(
        new_snapshots_table,
        exp.select("*").from_(snapshots_table),
        columns_to_types=snapshots_columns_to_types,
    )

    # Recreate the environments table and its indexes.
    engine_adapter.create_table(
        new_environments_table, environments_columns_to_types, primary_key=("name",)
    )
    engine_adapter.insert_append(
        new_environments_table,
        exp.select("*").from_(environments_table),
        columns_to_types=environments_columns_to_types,
    )

    # Recreate the intervals table and its indexes.
    engine_adapter.create_table(
        new_intervals_table, intervals_columns_to_types, primary_key=("id",)
    )
    engine_adapter.create_index(
        new_intervals_table, "_intervals_name_identifier_idx", ("name", "identifier")
    )
    engine_adapter.create_index(
        new_intervals_table, "_intervals_name_version_idx", ("name", "version")
    )
    engine_adapter.insert_append(
        new_intervals_table,
        exp.select("*").from_(intervals_table),
        columns_to_types=intervals_columns_to_types,
    )

    # Drop old tables.
    for table in (snapshots_table, environments_table, intervals_table):
        engine_adapter.drop_table(table)

    # Replace old tables with new ones.
    engine_adapter.rename_table(new_snapshots_table, snapshots_table)
    engine_adapter.rename_table(new_environments_table, environments_table)
    engine_adapter.rename_table(new_intervals_table, intervals_table)
