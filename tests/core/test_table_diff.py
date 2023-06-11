from sqlglot import exp

from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig


def test_data_diff(sushi_context_fixed_date):
    sushi_context_fixed_date.plan(
        "source_dev",
        no_prompts=True,
        auto_apply=True,
        skip_tests=True,
        start="2023-01-01",
        end="2023-01-31",
    )
    model = sushi_context_fixed_date.models["sushi.customer_revenue_by_day"]

    for column in model.query.find_all(exp.Column):
        if column.name == "total":
            column.replace(exp.condition("ot.total * 0.1"))

    model.query.select("1 AS z", copy=False).limit(10000, copy=False)
    sushi_context_fixed_date.upsert_model(model)

    sushi_context_fixed_date.auto_categorize_changes = CategorizerConfig(
        sql=AutoCategorizationMode.FULL
    )
    sushi_context_fixed_date.plan(
        "target_dev",
        create_from="source_dev",
        no_prompts=True,
        auto_apply=True,
        skip_tests=True,
        start="2023-01-01",
        end="2023-01-31",
    )

    diff = sushi_context_fixed_date.table_diff(
        source="source_dev",
        target="target_dev",
        on=exp.condition("s.customer_id = t.customer_id AND s.ds = t.ds"),
        model_or_snapshot="sushi.customer_revenue_by_day",
    )

    schema_diff = diff.schema_diff()
    assert schema_diff.added == [("z", exp.DataType.build("int"))]
    assert schema_diff.removed == []

    row_diff = diff.row_diff()
    assert row_diff.source_count == 546
    assert row_diff.target_count == 546
    assert row_diff.sample.shape == (20, 7)
