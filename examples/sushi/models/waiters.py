from macros.macros import incremental_by_ds  # type: ignore
from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import EmbeddedKind


@model(
    "sushi.waiters",
    is_sql=True,
    kind=EmbeddedKind(),
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
      date::DATE AS date
    FROM sushi.orders AS o
    WHERE @incremental_by_ds(ds)
    """
    if evaluator.runtime_stage != "loading":
        snapshot = evaluator.get_snapshot("sushi.waiters")
        assert snapshot is not None

        parent_snapshots = snapshot.parents
        assert len(parent_snapshots) == 1
        assert parent_snapshots[0].name.lower() == "sushi.orders"

    excluded = {"id", "customer_id", "start_ts", "end_ts"}
    projections = []
    for column, dtype in evaluator.columns_to_types("sushi.orders").items():
        if column not in excluded:
            projections.append(f"{column}::{dtype}")

    return (
        exp.select(*projections)
        .from_("sushi.orders AS o")
        .where(incremental_by_ds(evaluator, exp.to_column("date")))
        .distinct()
    )
