"""Move batch_size from the model and into the kind."""

import json

from sqlglot import exp


def migrate(state_sync, **kwargs):  # type: ignore
    snapshots_table = "_snapshots"
    if state_sync.schema:
        snapshots_table = f"{state_sync.schema}.{snapshots_table}"

    for row in state_sync.engine_adapter.fetchall(
        exp.select("*").from_(snapshots_table), quote_identifiers=True
    ):
        name, identifier, _, snapshot = row
        snapshot = json.loads(snapshot)
        model = snapshot["model"]
        if "batch_size" in model:
            batch_size = model.pop("batch_size")
            kind = model.get("kind")

            if kind:
                if kind["name"] in ("INCREMENTAL_BY_TIME_RANGE", "INCREMENTAL_BY_UNIQUE_KEY"):
                    kind["batch_size"] = batch_size

                    # this is not efficient, i'm doing this because i'm lazy and no one has snapshots at the time of writing this migration
                    # do not copy this code in future migrations

                    state_sync.engine_adapter.update_table(
                        snapshots_table,
                        {"snapshot": json.dumps(snapshot)},
                        where=f"name = '{name}' and identifier = '{identifier}'",
                    )
