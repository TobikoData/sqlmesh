import json
import logging

from sqlglot import exp

from sqlmesh.core.console import get_console


logger = logging.getLogger(__name__)
KEYS_TO_MAKE_DETERMINISTIC = ["__sqlmesh__vars__", "__sqlmesh__blueprint__vars__"]


def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    snapshots_table = "_snapshots"
    versions_table = "_versions"
    if schema:
        snapshots_table = f"{schema}.{snapshots_table}"
        versions_table = f"{schema}.{versions_table}"

    result = engine_adapter.fetchone(
        exp.select("schema_version").from_(versions_table), quote_identifiers=True
    )
    if not result:
        # This must be the first migration, so we can skip the check since the project was not exposed to 85 migration bug
        return
    schema_version = result[0]
    if schema_version < 85:
        # The project was not exposed to the bugged 85 migration, so we can skip it.
        return

    warning = (
        "SQLMesh detected that it may not be able to fully migrate the state database. This should not impact "
        "the migration process, but may result in unexpected changes being reported by the next `sqlmesh plan` "
        "command. Please run `sqlmesh diff prod` after the migration has completed, before making any new "
        "changes. If any unexpected changes are reported, consider running a forward-only plan to apply these "
        "changes and avoid unnecessary backfills: sqlmesh plan prod --forward-only. "
        "See https://sqlmesh.readthedocs.io/en/stable/concepts/plans/#forward-only-plans for more details.\n"
    )

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
                if (
                    key not in KEYS_TO_MAKE_DETERMINISTIC
                    and isinstance(executable, dict)
                    and executable.get("kind") == "value"
                ):
                    try:
                        parsed_value = eval(executable["payload"])
                        if isinstance(parsed_value, dict):
                            get_console().log_warning(warning)
                            return
                    except Exception:
                        logger.warning("Exception trying to eval payload", exc_info=True)
