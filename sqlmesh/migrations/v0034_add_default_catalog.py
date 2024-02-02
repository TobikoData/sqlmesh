"""Add default catalog to snapshots and update names to match new normalization rules."""

from __future__ import annotations

import json
import typing as t

import pandas as pd
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.helper import dict_depth, seq_get
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.utils.migration import index_text_type


def set_default_catalog(
    table: exp.Table,
    default_catalog: t.Optional[str],
) -> exp.Table:
    if default_catalog and not table.catalog and table.db:
        table.set("catalog", exp.parse_identifier(default_catalog))

    return table


def normalize_model_name(
    table: str | exp.Table,
    default_catalog: t.Optional[str],
    dialect: DialectType = None,
) -> str:
    table = exp.to_table(table, dialect=dialect)

    table = set_default_catalog(table, default_catalog)
    return exp.table_name(normalize_identifiers(table, dialect=dialect), identify=True)


def normalize_mapping_schema(mapping_schema: t.Dict, dialect: str) -> t.Dict:
    # Example input: {'"catalog"': {'schema': {'table': {'column': 'INT'}}}}
    # Example output: {'"catalog"': {'"schema"': {'"table"': {'column': 'INT'}}}}
    normalized_mapping_schema = {}
    for key, value in mapping_schema.items():
        if isinstance(value, dict):
            normalized_mapping_schema[normalize_model_name(key, None, dialect)] = (
                normalize_mapping_schema(value, dialect)
            )
        else:
            normalized_mapping_schema[key] = value
    return normalized_mapping_schema


def update_dbt_relations(
    source: t.Optional[t.Dict], keys: t.List[str], default_catalog: t.Optional[str]
) -> None:
    if not default_catalog or not source:
        return
    for key in keys:
        relations = source.get(key)
        if relations:
            relations = [relations] if "database" in relations else relations.values()
            for relation in relations:
                if not relation["database"]:
                    relation["database"] = default_catalog


def migrate(state_sync, default_catalog: t.Optional[str], **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    environments_table = "_environments"
    intervals_table = "_intervals"
    seeds_table = "_seeds"

    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"
        environments_table = f"{schema}.{environments_table}"
        intervals_table = f"{schema}.{intervals_table}"
        seeds_table = f"{schema}.{seeds_table}"

    new_snapshots = []
    snapshot_to_dialect = {}
    index_type = index_text_type(engine_adapter.dialect)

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        # This is here in the case where the user originally had catalog in this model name, and therefore
        # we would have before created the table with the catalog in the name. New logic removes the catalog,
        # and therefore we need to make sure the table name is the same as the original table name, so we include
        # this override
        parsed_snapshot["base_table_name_override"] = parsed_snapshot["name"]
        node = parsed_snapshot["node"]
        dialect = node.get("dialect")
        normalized_name = (
            normalize_model_name(name, default_catalog=default_catalog, dialect=dialect)
            if node["source_type"] != "audit"
            else name
        )
        parsed_snapshot["name"] = normalized_name
        # At the time of migration all nodes had default catalog, so we don't have to check type
        node["default_catalog"] = default_catalog
        snapshot_to_dialect[name] = dialect
        mapping_schema = node.get("mapping_schema", {})
        if mapping_schema:
            normalized_default_catalog = (
                normalize_model_name(default_catalog, default_catalog=None, dialect=dialect)
                if default_catalog
                else None
            )
            mapping_schema_depth = dict_depth(mapping_schema)
            if mapping_schema_depth == 3 and normalized_default_catalog:
                mapping_schema = {normalized_default_catalog: mapping_schema}
            node["mapping_schema"] = normalize_mapping_schema(mapping_schema, dialect)
        depends_on = node.get("depends_on", [])
        if depends_on:
            node["depends_on"] = [
                normalize_model_name(dep, default_catalog, dialect) for dep in depends_on
            ]
        if parsed_snapshot["parents"]:
            parsed_snapshot["parents"] = [
                {
                    "name": normalize_model_name(parent["name"], default_catalog, dialect),
                    "identifier": parent["identifier"],
                }
                for parent in parsed_snapshot["parents"]
            ]
        if parsed_snapshot["indirect_versions"]:
            parsed_snapshot["indirect_versions"] = {
                normalize_model_name(name, default_catalog, dialect): snapshot_data_versions
                for name, snapshot_data_versions in parsed_snapshot["indirect_versions"].items()
            }
        # dbt specific migration
        jinja_macros = node.get("jinja_macros")
        if (
            default_catalog
            and jinja_macros
            and jinja_macros.get("create_builtins_module") == "sqlmesh.dbt"
        ):
            update_dbt_relations(
                jinja_macros.get("global_objs"), ["refs", "sources", "this"], default_catalog
            )

        new_snapshots.append(
            {
                "name": normalized_name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
                "kind_name": kind_name,
            }
        )

    if new_snapshots:
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
            contains_json=True,
        )

    new_environments = []
    default_dialect = seq_get(list(snapshot_to_dialect.values()), 0)
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
    ) in engine_adapter.fetchall(
        exp.select(
            "name",
            "snapshots",
            "start_at",
            "end_at",
            "plan_id",
            "previous_plan_id",
            "expiration_ts",
            "finalized_ts",
            "promoted_snapshot_ids",
            "suffix_target",
        ).from_(environments_table),
        quote_identifiers=True,
    ):
        new_snapshots = []
        for snapshot in json.loads(snapshots):
            snapshot_name = snapshot["name"]
            snapshot["base_table_name_override"] = snapshot_name
            dialect = snapshot_to_dialect.get(snapshot_name, default_dialect)
            node_type = snapshot.get("node_type")
            normalized_name = (
                normalize_model_name(snapshot_name, default_catalog, dialect)
                if node_type is None or node_type == "model"
                else snapshot_name
            )
            snapshot["name"] = normalized_name
            if snapshot["parents"]:
                snapshot["parents"] = [
                    {
                        "name": normalize_model_name(parent["name"], default_catalog, dialect),
                        "identifier": parent["identifier"],
                    }
                    for parent in snapshot["parents"]
                ]
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
                "promoted_snapshot_ids": promoted_snapshot_ids,
                "suffix_target": suffix_target,
            }
        )

    if new_environments:
        engine_adapter.delete_from(environments_table, "TRUE")

        engine_adapter.insert_append(
            environments_table,
            pd.DataFrame(new_environments),
            columns_to_types={
                "name": exp.DataType.build(index_type),
                "snapshots": exp.DataType.build("text"),
                "start_at": exp.DataType.build("text"),
                "end_at": exp.DataType.build("text"),
                "plan_id": exp.DataType.build("text"),
                "previous_plan_id": exp.DataType.build("text"),
                "expiration_ts": exp.DataType.build("bigint"),
                "finalized_ts": exp.DataType.build("bigint"),
                "promoted_snapshot_ids": exp.DataType.build("text"),
                "suffix_target": exp.DataType.build("text"),
            },
            contains_json=True,
        )

        # We update environment to not be finalized in order to force them to update their views
        # in order to make sure the views now have the fully qualified names
        # We only do this if a default catalog was applied otherwise the current views are fine
        # We do this post creating the new environments in order to avoid having to find a way to
        # expression a null timestamp value in pandas that works across all engines
        if default_catalog:
            engine_adapter.execute(
                exp.update(environments_table, {"finalized_ts": None}, where="1=1"),
                quote_identifiers=True,
            )

    new_intervals = []
    for (
        id,
        created_ts,
        name,
        identifier,
        version,
        start_ts,
        end_ts,
        is_dev,
        is_removed,
        is_compacted,
    ) in engine_adapter.fetchall(
        exp.select(
            "id",
            "created_ts",
            "name",
            "identifier",
            "version",
            "start_ts",
            "end_ts",
            "is_dev",
            "is_removed",
            "is_compacted",
        ).from_(intervals_table),
        quote_identifiers=True,
    ):
        dialect = snapshot_to_dialect.get(name, default_dialect)
        normalized_name = normalize_model_name(name, default_catalog, dialect)
        new_intervals.append(
            {
                "id": id,
                "created_ts": created_ts,
                "name": normalized_name,
                "identifier": identifier,
                "version": version,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "is_dev": is_dev,
                "is_removed": is_removed,
                "is_compacted": is_compacted,
            }
        )

    if new_intervals:
        engine_adapter.delete_from(intervals_table, "TRUE")

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
            contains_json=False,
        )

    new_seeds = []
    for (
        name,
        identifier,
        content,
    ) in engine_adapter.fetchall(
        exp.select(
            "name",
            "identifier",
            "content",
        ).from_(seeds_table),
        quote_identifiers=True,
    ):
        dialect = snapshot_to_dialect.get(name, default_dialect)
        normalized_name = normalize_model_name(name, default_catalog, dialect)
        new_seeds.append(
            {
                "name": normalized_name,
                "identifier": identifier,
                "content": content,
            }
        )

    if new_seeds:
        engine_adapter.delete_from(seeds_table, "TRUE")

        engine_adapter.insert_append(
            seeds_table,
            pd.DataFrame(new_seeds),
            columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "content": exp.DataType.build("text"),
            },
            contains_json=False,
        )
