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
      ds::TEXT AS ds
    FROM sushi.orders AS o
    WHERE @incremental_by_ds(ds)
    """
    return (
        exp.select("waiter_id::int as waiter_id", "ds::text as ds")
        .from_("sushi.orders AS o")
        .where(incremental_by_ds(evaluator, exp.to_column("ds")))
        .distinct()
    )
