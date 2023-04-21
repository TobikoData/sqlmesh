import json


def migrate(state_sync):  # type: ignore
    """Move batch_size from the model and into the kind."""

    snapshots_table = f"{state_sync.schema}._snapshots"

    for row in state_sync.engine_adapter.fetchall(f"SELECT * FROM {snapshots_table}"):
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
                        contains_json=True,
                    )
