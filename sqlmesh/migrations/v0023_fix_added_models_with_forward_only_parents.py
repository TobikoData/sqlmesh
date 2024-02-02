"""Fix snapshots of added models with forward only parents."""

import json
import typing as t

from sqlglot import exp

from sqlmesh.utils.dag import DAG


def migrate(state_sync: t.Any, **kwargs) -> None:  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    environments_table = "_environments"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"
        environments_table = f"{schema}.{environments_table}"

    dag: DAG[t.Tuple[str, str]] = DAG()
    snapshot_mapping: t.Dict[t.Tuple[str, str], t.Dict[str, t.Any]] = {}

    for identifier, snapshot in engine_adapter.fetchall(
        exp.select("identifier", "snapshot").from_(snapshots_table),
        quote_identifiers=True,
    ):
        parsed_snapshot = json.loads(snapshot)

        snapshot_id = (parsed_snapshot["name"], identifier)
        snapshot_mapping[snapshot_id] = parsed_snapshot

        parent_ids = [
            (parent["name"], parent["identifier"]) for parent in parsed_snapshot["parents"]
        ]
        dag.add(snapshot_id, parent_ids)

    snapshots_to_delete = set()

    for snapshot_id in dag:
        if snapshot_id not in snapshot_mapping:
            continue
        parsed_snapshot = snapshot_mapping[snapshot_id]
        is_breaking = parsed_snapshot.get("change_category") == 1
        has_previous_versions = bool(parsed_snapshot.get("previous_versions", []))

        has_paused_forward_only_parent = False
        if is_breaking and not has_previous_versions:
            for upstream_id in dag.upstream(snapshot_id):
                if upstream_id not in snapshot_mapping:
                    continue
                upstream_snapshot = snapshot_mapping[upstream_id]
                upstream_change_category = upstream_snapshot.get("change_category")
                is_forward_only_upstream = upstream_change_category == 3
                if is_forward_only_upstream and not upstream_snapshot.get("unpaused_ts"):
                    has_paused_forward_only_parent = True
                    break

        if has_paused_forward_only_parent:
            snapshots_to_delete.add(snapshot_id)

    if snapshots_to_delete:
        where = t.cast(exp.Tuple, exp.convert((exp.column("name"), exp.column("identifier")))).isin(
            *snapshots_to_delete
        )
        engine_adapter.delete_from(snapshots_table, where)
