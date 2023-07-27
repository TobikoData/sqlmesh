import pytest

from sqlmesh.core import dialect as d
from sqlmesh.core.metric import expand_metrics, load_metric_ddl
from sqlmesh.utils.errors import ConfigError


def test_load_metric_ddl():
    a = d.parse_one(
        """
    -- description a
    METRIC (
        name A,
        expression SUM(x),
        owner b
    );
    """
    )

    meta = load_metric_ddl(a, dialect="")
    assert meta.name == "a"
    assert meta.expression.sql() == "SUM(x)"
    assert meta.owner == "b"
    assert meta.description == "description a"


def test_load_invalid():
    with pytest.raises(
        ConfigError, match=r"Only METRIC\(...\) statements are allowed. Found SELECT"
    ):
        load_metric_ddl(
            d.parse_one(
                """
                SELECT 1;
                """
            ),
            dialect="",
        )

    with pytest.raises(ConfigError, match=r"Metric 'a' missing an aggregation or metric ref."):
        load_metric_ddl(
            d.parse_one(
                """
                METRIC (
                    name a,
                    expression 1
                )
                """
            ),
            dialect="",
        ).to_metric({}, {})


def test_expand_metrics():
    expressions = d.parse(
        """
    -- description a
    METRIC (
        name a,
        expression SUM(model.x),
        owner b
    );

    -- description b
    METRIC (
        name b,
        expression COUNT(DISTINCT model.y),
        owner b
    );

    -- description c
    METRIC (
        name c,
        expression a / b,
        owner b
    );

    -- description d
    METRIC (
        name d,
        expression c + 1,
        owner b
    );
    """
    )

    metas = {}

    for expr in expressions:
        meta = load_metric_ddl(expr, dialect="")
        metas[meta.name] = meta

    metrics = expand_metrics(metas)

    metric_a = metrics["a"]
    assert metric_a.name == "a"
    assert metric_a.expression.sql() == "SUM(model.x)"
    assert metric_a.expanded.sql() == "SUM(model.x) AS a"
    assert metric_a.formula.sql() == "a AS a"
    assert metric_a.owner == "b"
    assert metric_a.description == "description a"

    metric_b = metrics["b"]
    assert metric_b.name == "b"
    assert metric_b.expression.sql() == "COUNT(DISTINCT model.y)"
    assert metric_b.expanded.sql() == "COUNT(DISTINCT model.y) AS b"
    assert metric_b.formula.sql() == "b AS b"

    metric_c = metrics["c"]
    assert metric_c.name == "c"
    assert metric_c.expression.sql() == "a / b"
    assert metric_c.expanded.sql() == "SUM(model.x) AS a / COUNT(DISTINCT model.y) AS b"
    assert metric_c.formula.sql() == "a / b AS c"

    metric_d = metrics["d"]
    assert metric_d.expression.sql() == "c + 1"
    assert metric_d.expanded.sql() == "SUM(model.x) AS a / COUNT(DISTINCT model.y) AS b + 1"
    assert metric_d.formula.sql() == "a / b + 1 AS d"

    assert metric_d.aggs == {
        d.parse_one("SUM(model.x) AS a"): {"model"},
        d.parse_one("COUNT(DISTINCT model.y) AS b"): {"model"},
    }
