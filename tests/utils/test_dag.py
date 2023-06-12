from sqlmesh.utils.dag import DAG


def test_downstream(sushi_context):
    assert set(sushi_context.dag.downstream("sushi.order_items")) == {
        "sushi.customer_revenue_by_day",
        "sushi.customer_revenue_lifetime",
        "sushi.top_waiters",
        "sushi.waiter_revenue_by_day",
    }


def test_no_downstream(sushi_context):
    assert sushi_context.dag.downstream("sushi.top_waiters") == []


def test_lineage(sushi_context):
    lineage = sushi_context.dag.lineage("sushi.order_items").sorted
    assert lineage.index("sushi.order_items") > lineage.index("sushi.items")
    assert lineage.index("sushi.customer_revenue_by_day") > lineage.index("sushi.order_items")
    assert lineage.index("sushi.waiter_revenue_by_day") > lineage.index("sushi.order_items")


def test_sorted():
    dag = DAG({"a": {"b", "c"}, "b": {"d", "e"}, "c": {"f", "g"}})
    result = dag.sorted

    assert len(set(result)) == 7
    assert result[0] in ("d", "e", "f", "g")
    assert result[1] in ("d", "e", "f", "g")
    assert result[2] in ("d", "e", "f", "g")
    assert result[3] in ("d", "e", "f", "g")
    assert result[4] in ("b", "c")
    assert result[5] in ("b", "c")
    assert result[6] == "a"


def test_reversed_graph():
    dag = DAG({"a": {}, "b": {"a"}, "c": {"b", "a"}, "d": {}})

    assert dag.reversed.graph == {
        "a": {"b", "c"},
        "b": {"c"},
        "c": set(),
        "d": set(),
    }
