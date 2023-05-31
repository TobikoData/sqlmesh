from sqlglot import exp

from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig


def test_data_diff(sushi_context):
    model = sushi_context.models["sushi.customer_revenue_by_day"]

    for column in model.query.find_all(exp.Column):
        if column.name == "total":
            column.replace(exp.condition("ot.total * 0.1"))

    model.query.select("1 AS z", copy=False).limit(10000, copy=False)
    sushi_context.upsert_model(model)

    sushi_context.auto_categorize_changes = CategorizerConfig(sql=AutoCategorizationMode.FULL)
    sushi_context.plan("dev", no_prompts=True, auto_apply=True, skip_tests=True)

    diff = sushi_context.table_diff(
        source="prod",
        target="dev",
        on=exp.condition("s.customer_id = t.customer_id"),
        model_or_snapshot="sushi.customer_revenue_by_day",
    )

    schema_diff = diff.schema_diff()
    assert schema_diff.added == [("z", exp.DataType.build("int"))]
    assert schema_diff.removed == []

    row_diff = diff.row_diff()
    assert row_diff.source_count == 2299
    assert row_diff.target_count == 2675
    assert row_diff.sample.shape == (20, 7)
