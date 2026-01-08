"""The baseline migration script that sets up the initial state tables."""

from sqlglot import exp
from sqlmesh.utils.migration import blob_text_type, index_text_type


def migrate_schemas(engine_adapter, schema, **kwargs):  # type: ignore
    intervals_table = "_intervals"
    snapshots_table = "_snapshots"
    environments_table = "_environments"
    versions_table = "_versions"
    if schema:
        engine_adapter.create_schema(schema)
        intervals_table = f"{schema}.{intervals_table}"
        snapshots_table = f"{schema}.{snapshots_table}"
        environments_table = f"{schema}.{environments_table}"
        versions_table = f"{schema}.{versions_table}"

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

    versions_columns_to_types = {
        "schema_version": exp.DataType.build("int"),
        "sqlglot_version": exp.DataType.build(index_type),
        "sqlmesh_version": exp.DataType.build(index_type),
    }

    # Create the versions table.
    engine_adapter.create_state_table(versions_table, versions_columns_to_types)

    # Create the snapshots table and its indexes.
    engine_adapter.create_state_table(
        snapshots_table, snapshots_columns_to_types, primary_key=("name", "identifier")
    )
    engine_adapter.create_index(snapshots_table, "_snapshots_name_version_idx", ("name", "version"))

    # Create the environments table and its indexes.
    engine_adapter.create_state_table(
        environments_table, environments_columns_to_types, primary_key=("name",)
    )

    # Create the intervals table and its indexes.
    engine_adapter.create_state_table(
        intervals_table, intervals_columns_to_types, primary_key=("id",)
    )
    engine_adapter.create_index(
        intervals_table, "_intervals_name_identifier_idx", ("name", "identifier")
    )
    engine_adapter.create_index(intervals_table, "_intervals_name_version_idx", ("name", "version"))


def migrate_rows(engine_adapter, schema, **kwargs):  # type: ignore
    pass
