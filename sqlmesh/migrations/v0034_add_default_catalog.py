"""Use default catalog to snapshots."""

import json
import typing as t

import pandas as pd
from sqlglot import MappingSchema, exp
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
    table_name: str,
    default_catalog: t.Optional[str],
    dialect: t.Optional[str] = None,
) -> str:
    table = exp.to_table(table_name, dialect=dialect)

    table = set_default_catalog(table, default_catalog)
    return exp.table_name(normalize_identifiers(table, dialect=dialect))


def migrate(state_sync, default_catalog: t.Optional[str], **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    environments_table = "_environments"

    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"
        environments_table = f"{schema}.{environments_table}"

    new_snapshots = []
    snapshot_to_dialect = {}

    for name, identifier, version, snapshot, kind_name in engine_adapter.fetchall(
        exp.select("name", "identifier", "version", "snapshot", "kind_name").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)
        node = parsed_snapshot["node"]
        # At the time of migration all nodes had default catalog so we don't have to check type
        node["default_catalog"] = default_catalog
        dialect = node.get("dialect")
        if node["source_type"] != "audit":
            node["fqn_"] = normalize_model_name(name, default_catalog, dialect)
        snapshot_to_dialect[name] = dialect
        mapping_schema = MappingSchema(node.get("mapping_schema", {}))
        if mapping_schema.depth() == 2:
            parsed_snapshot["node"]["mapping_schema"] = {default_catalog: mapping_schema.mapping}
        depends_on = node.get("depends_on", [])
        if depends_on:
            node["depends_on"] = [
                normalize_model_name(dep, default_catalog, dialect) for dep in depends_on
            ]
        new_snapshots.append(
            {
                "name": name,
                "identifier": identifier,
                "version": version,
                "snapshot": json.dumps(parsed_snapshot),
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
        default_dialect = list(snapshot_to_dialect.values())[0]
        for snapshot in json.loads(snapshots):
            snapshot["fqn"] = normalize_model_name(
                snapshot["name"], default_catalog, snapshot_to_dialect.get(name, default_dialect)
            )
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

        index_type = index_text_type(engine_adapter.dialect)

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
        # We do this post creating the new environments in order to avoid having to find a way to
        # expression a null timestamp value in pandas that works across all engines
        engine_adapter.execute(
            exp.update(environments_table, {"finalized_ts": None}),
            quote_identifiers=True,
        )
