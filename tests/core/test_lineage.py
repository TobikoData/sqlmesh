from sqlmesh.core import dialect as d
from sqlmesh.core.config import Config
from sqlmesh.core.config.model import ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.core.lineage import column_dependencies, column_description, lineage
from sqlmesh.core.model import load_sql_based_model


def test_column_dependencies(sushi_context_pre_scheduling):
    context = sushi_context_pre_scheduling
    assert column_dependencies(context, "sushi.waiter_revenue_by_day", "revenue") == {
        '"memory"."sushi"."items"': {"price"},
        '"memory"."sushi"."order_items"': {"quantity"},
    }


def test_column_description(sushi_context_pre_scheduling):
    context = sushi_context_pre_scheduling
    assert column_description(context, "sushi.top_waiters", "waiter_id") == "Waiter id"


def test_lineage():
    context = Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="snowflake")))

    model = load_sql_based_model(
        d.parse(
            """
        MODEL (name db.model1);

        SELECT "A"
        FROM (
            SELECT 1 a
        ) x
        """
        ),
    )

    context.upsert_model(model)
    node = lineage('"A"', model)
    assert node.name == "A"
