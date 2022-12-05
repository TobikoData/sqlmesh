def test_downstream(sushi_context):
    assert set(sushi_context.dag.downstream("sushi.order_items")) == {
        "sushi.customer_revenue_by_day",
        "sushi.top_waiters",
        "sushi.waiter_revenue_by_day",
    }


def test_no_downstream(sushi_context):
    assert sushi_context.dag.downstream("sushi.customers") == []


def test_lineage(sushi_context):
    lineage = sushi_context.dag.lineage("sushi.order_items").sort()
    assert lineage.index("sushi.order_items") > lineage.index("raw.order_items")
    assert lineage.index("sushi.customer_revenue_by_day") > lineage.index(
        "sushi.order_items"
    )
    assert lineage.index("sushi.waiter_revenue_by_day") > lineage.index(
        "sushi.order_items"
    )
