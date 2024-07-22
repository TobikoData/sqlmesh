import pytest

from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import SQLMeshError


def test_downstream(sushi_context):
    assert set(sushi_context.dag.downstream('"memory"."sushi"."order_items"')) == {
        '"memory"."sushi"."customer_revenue_by_day"',
        '"memory"."sushi"."customer_revenue_lifetime"',
        '"memory"."sushi"."top_waiters"',
        '"memory"."sushi"."waiter_revenue_by_day"',
    }


def test_no_downstream(sushi_context):
    assert sushi_context.dag.downstream("memory.sushi.top_waiters") == []


def test_lineage(sushi_context):
    lineage = sushi_context.dag.lineage('"memory"."sushi"."order_items"').sorted
    assert lineage.index('"memory"."sushi"."order_items"') > lineage.index(
        '"memory"."sushi"."items"'
    )
    assert lineage.index('"memory"."sushi"."customer_revenue_by_day"') > lineage.index(
        '"memory"."sushi"."order_items"'
    )
    assert lineage.index('"memory"."sushi"."waiter_revenue_by_day"') > lineage.index(
        '"memory"."sushi"."order_items"'
    )


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


def test_upstream():
    dag = DAG({"a": {"b", "c"}, "b": {"d", "e"}, "c": {"f", "g"}})
    assert dag.upstream("a") == {"b", "c", "d", "e", "f", "g"}


def test_sorted_with_cycles():
    dag = DAG({"a": {}, "b": {"a"}, "c": {"b"}, "d": {"b", "e"}, "e": {"b", "d"}})

    with pytest.raises(SQLMeshError) as ex:
        dag.sorted

    expected_error_message = (
        "Detected a cycle in the DAG. Please make sure there are no circular references between nodes.\n"
        "Last nodes added to the DAG: c\n"
        "Possible candidates to check for circular references: d, e"
    )

    assert expected_error_message == str(ex.value)

    dag = DAG({"a": {"b"}, "b": {"c"}, "c": {"a"}})

    with pytest.raises(SQLMeshError) as ex:
        dag.sorted

    expected_error_message = (
        "Detected a cycle in the DAG. Please make sure there are no circular references between nodes.\n"
        "Possible candidates to check for circular references: a, b, c"
    )

    assert expected_error_message == str(ex.value)

    dag = DAG({"a": {}, "b": {"a", "d"}, "c": {"a"}, "d": {"b"}})

    with pytest.raises(SQLMeshError) as ex:
        dag.sorted

    expected_error_message = (
        "Last nodes added to the DAG: c\n"
        "Possible candidates to check for circular references: b, d"
    )

    assert expected_error_message in str(ex.value)


def test_reversed_graph():
    dag = DAG({"a": {}, "b": {"a"}, "c": {"b", "a"}, "d": {}})

    assert dag.reversed.graph == {
        "a": {"b", "c"},
        "b": {"c"},
        "c": set(),
        "d": set(),
    }


def test_prune():
    assert DAG({"a": {"b", "d"}, "b": {"d", "e"}}).prune("a", "d").graph == {
        "a": {"d"},
        "d": set(),
    }
