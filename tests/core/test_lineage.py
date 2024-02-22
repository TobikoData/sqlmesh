from sqlmesh.core.lineage import column_dependencies, column_description


def test_column_dependencies(sushi_context_pre_scheduling):
    context = sushi_context_pre_scheduling
    assert column_dependencies(context, "sushi.waiter_revenue_by_day", "revenue") == {
        '"memory"."sushi"."items"': {"price"},
        '"memory"."sushi"."order_items"': {"quantity"},
    }


def test_column_description(sushi_context_pre_scheduling):
    context = sushi_context_pre_scheduling
    assert column_description(context, "sushi.top_waiters", "waiter_id") == "Waiter id"
