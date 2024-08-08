from macros.macros import incremental_by_ds  # type: ignore
from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName


@model(
    "sushi.waiters",
    is_sql=True,
    kind=dict(name=ModelKindName.EMBEDDED),
    owner="jen",
    cron="@daily",
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Select:
    """
    This creates the following model using SQLGlot's builder methods:

    MODEL (
      name sushi.waiters,
      kind EMBEDDED,
      owner jen,
      cron '@daily',
    );

    SELECT DISTINCT
      waiter_id::INT AS waiter_id,
      event_date::DATE AS event_date
    FROM sushi.orders AS o
    WHERE @incremental_by_ds(ds)
    """
    if evaluator.runtime_stage != "loading":
        snapshot = evaluator.get_snapshot("sushi.waiters")
        assert snapshot is not None

        parent_snapshots = snapshot.parents
        assert len(parent_snapshots) == 1
        name = '"sushi"."orders"'

        # There are tests which force not having a default catalog so we check here if one is defined
        # and add it to the name if it is
        default_catalog = evaluator.default_catalog
        parent_snapshot_name = parent_snapshots[0].name
        parent_snapshot_catalog = parent_snapshot_name.split(".")[0].strip('"')

        if default_catalog:
            # make sure we don't double quote the default catalog
            default_catalog = default_catalog.strip('"')

            # Snowflake normalizes unquoted names to uppercase, which can cause case mismatches with
            # default_catalog due to sqlmesh's default normalization behavior. SQLMesh addresses this
            # by rewriting the default catalog name on the fly in snowflake `_to_sql()`. This model
            # code manually extracts default_catalog name, so we manually lowercase the default_catalog
            # if the parent catalog name is an uppercase version of the default catalog name.
            default_catalog = (
                default_catalog.lower()
                if parent_snapshot_catalog == default_catalog.lower()
                else default_catalog
            )

            name = ".".join([f'"{default_catalog}"', name])

        assert parent_snapshot_name == name, f"Snapshot Name: {parent_snapshot_name}, Name: {name}"

    excluded = {"id", "customer_id", "start_ts", "end_ts"}
    projections = []
    for column, dtype in evaluator.columns_to_types("sushi.orders").items():
        if column not in excluded:
            projections.append(f"{column}::{dtype}")

    return (
        exp.select(*projections)
        .from_("sushi.orders AS o")
        .where(incremental_by_ds(evaluator, exp.to_column("event_date")))
        .distinct()
    )
