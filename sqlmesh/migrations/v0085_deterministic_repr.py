"""
When serializing some objects, like `__sqlmesh__vars__`, the order of keys in the dictionary were not deterministic
and therefore this migration applies deterministic sorting to the keys of the dictionary.
"""

import json
import logging
import typing as t
from dataclasses import dataclass

from sqlglot import exp

from sqlmesh.utils.migration import index_text_type, blob_text_type


logger = logging.getLogger(__name__)


KEYS_TO_MAKE_DETERMINISTIC = ["__sqlmesh__vars__", "__sqlmesh__blueprint__vars__"]


# Make sure `SqlValue` is defined so it can be used by `eval` call in the migration
@dataclass
class SqlValue:
    """A SQL string representing a generated SQLGlot AST."""

    sql: str


def _dict_sort(obj: t.Any) -> str:
    try:
        if isinstance(obj, dict):
            obj = dict(sorted(obj.items(), key=lambda x: str(x[0])))
    except Exception:
        logger.warning("Failed to sort non-recursive dict", exc_info=True)
    return repr(obj)


def migrate(state_sync, **kwargs):  # type: ignore
    import pandas as pd

    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"

    migration_needed = False
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
        python_env = parsed_snapshot["node"].get("python_env")

        if python_env:
            for key, executable in python_env.items():
                if key not in KEYS_TO_MAKE_DETERMINISTIC:
                    continue
                if isinstance(executable, dict) and executable.get("kind") == "value":
                    old_payload = executable["payload"]
                    try:
                        # Try to parse the old payload and re-serialize it deterministically
                        parsed_value = eval(old_payload)
                        new_payload = _dict_sort(parsed_value)

                        # Only update if the representation changed
                        if old_payload != new_payload:
                            executable["payload"] = new_payload
                            migration_needed = True
                    except Exception:
                        # If we still can't eval it, leave it as-is
                        logger.warning("Exception trying to eval payload", exc_info=True)

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

    if migration_needed and new_snapshots:
        engine_adapter.delete_from(snapshots_table, "TRUE")

        index_type = index_text_type(engine_adapter.dialect)
        blob_type = blob_text_type(engine_adapter.dialect)

        engine_adapter.insert_append(
            snapshots_table,
            pd.DataFrame(new_snapshots),
            target_columns_to_types={
                "name": exp.DataType.build(index_type),
                "identifier": exp.DataType.build(index_type),
                "version": exp.DataType.build(index_type),
                "snapshot": exp.DataType.build(blob_type),
                "kind_name": exp.DataType.build("text"),
                "updated_ts": exp.DataType.build("bigint"),
                "unpaused_ts": exp.DataType.build("bigint"),
                "ttl_ms": exp.DataType.build("bigint"),
                "unrestorable": exp.DataType.build("boolean"),
            },
        )
