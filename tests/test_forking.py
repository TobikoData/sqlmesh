import os
import pytest

from sqlmesh import Context
from sqlmesh.core import loader


pytestmark = pytest.mark.isolated


def test_parallel_load(assert_exp_eq, mocker):
    mocker.patch("sqlmesh.core.constants.MAX_FORK_WORKERS", 2)
    spy = mocker.spy(loader, "_update_model_schemas_parallel")
    context = Context(paths="examples/sushi")

    if hasattr(os, "fork"):
        spy.assert_called()

    assert_exp_eq(
        context.render("sushi.customers"),
        """
WITH "current_marketing" AS (
  SELECT
    "marketing"."customer_id" AS "customer_id",
    "marketing"."status" AS "status"
  FROM "memory"."sushi"."marketing" AS "marketing"
  WHERE
    "marketing"."valid_to" IS NULL
)
SELECT DISTINCT
  CAST("o"."customer_id" AS INT) AS "customer_id", /* this comment should not be registered */
  "m"."status" AS "status",
  "d"."zip" AS "zip"
FROM "memory"."sushi"."orders" AS "o"
LEFT JOIN "current_marketing" AS "m"
  ON "m"."customer_id" = "o"."customer_id"
LEFT JOIN "memory"."raw"."demographics" AS "d"
  ON "d"."customer_id" = "o"."customer_id"
        """,
    )

    context.plan(no_prompts=True, auto_apply=True)
