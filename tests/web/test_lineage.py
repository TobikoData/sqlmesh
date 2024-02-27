from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from sqlmesh.core.context import Context
from web.server.main import app

pytestmark = pytest.mark.web

client = TestClient(app)


def test_get_lineage(web_sushi_context: Context) -> None:
    response = client.get("/api/lineage/sushi.waiters/event_date")

    assert response.status_code == 200
    assert response.json() == {
        '"memory"."sushi"."waiters"': {
            "event_date": {
                "source": """SELECT DISTINCT
  CAST(o.event_date AS DATE) AS event_date
FROM memory.sushi.orders AS o
WHERE
  o.event_date <= CAST('1970-01-01' AS DATE)
  AND o.event_date >= CAST('1970-01-01' AS DATE)""",
                "expression": "CAST(o.event_date AS DATE) AS event_date",
                "models": {'"memory"."sushi"."orders"': ["event_date"]},
            }
        },
        '"memory"."sushi"."orders"': {
            "event_date": {
                "source": "SELECT\n  CAST(NULL AS DATE) AS event_date\nFROM (VALUES\n  (1)) AS t(dummy)",
                "expression": "CAST(NULL AS DATE) AS event_date",
                "models": {},
            }
        },
    }


def test_get_lineage_managed_columns(web_sushi_context: Context) -> None:
    # Get lineage with upstream managed columns
    response = client.get("/api/lineage/sushi.customers/customer_id")
    assert response.status_code == 200
    assert "valid_from" in response.text
    assert "valid_to" in response.text

    # Get lineage of managed column
    response = client.get("/api/lineage/sushi.marketing/valid_from")
    assert response.status_code == 200
    assert response.json() == {
        '"memory"."sushi"."marketing"': {
            "valid_from": {
                "source": """SELECT
  CAST(NULL AS TIMESTAMP) AS valid_from
FROM memory.sushi.raw_marketing""",
                "expression": "CAST(NULL AS TIMESTAMP) AS valid_from",
                "models": {},
            }
        }
    }


def test_get_lineage_single_model(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text("MODEL (name foo); SELECT col FROM bar;")
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {}


def test_get_lineage_external_model(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text("MODEL (name foo); SELECT col FROM bar;")
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text("MODEL (name bar); SELECT * FROM baz;")
    baz_sql_file = models_dir / "baz.sql"
    baz_sql_file.write_text("MODEL (name baz); SELECT * FROM external_table;")
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {}


def test_get_lineage_cte(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           WITH my_cte AS (
               SELECT col FROM bar
           )
           SELECT col FROM my_cte;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT col FROM baz;"""
    )
    baz_sql_file = models_dir / "baz.sql"
    baz_sql_file.write_text(
        """MODEL (name baz);
           SELECT col FROM external_table;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {"my_cte": ["col"]}
    assert response_json["my_cte"]["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_cte_downstream(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT col FROM bar;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           WITH my_cte AS (
               SELECT col FROM baz
           )
           SELECT col FROM my_cte;"""
    )
    baz_sql_file = models_dir / "baz.sql"
    baz_sql_file.write_text(
        """MODEL (name baz);
           SELECT col FROM external_table;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {"my_cte": ["col"]}
    assert response_json["my_cte"]["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_join(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT id, bar.quantity * baz.price AS col FROM bar JOIN baz ON bar.id = baz.id;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT id, quantity FROM external_bar;"""
    )
    baz_sql_file = models_dir / "baz.sql"
    baz_sql_file.write_text(
        """MODEL (name baz);
           SELECT id, price FROM external_baz;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["quantity"], '"baz"': ["price"]}
    assert response_json['"bar"']["quantity"]["models"] == {'"external_bar"': ["quantity"]}
    assert response_json['"baz"']["price"]["models"] == {'"external_baz"': ["price"]}


def test_get_lineage_multiple_columns(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT id, bar.value * bar.multiplier AS col FROM bar;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT id, value, multiplier FROM external_bar;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert "value" in response_json['"foo"']["col"]["models"]['"bar"']
    assert "multiplier" in response_json['"foo"']["col"]["models"]['"bar"']
    assert response_json['"bar"']["value"]["models"] == {'"external_bar"': ["value"]}
    assert response_json['"bar"']["multiplier"]["models"] == {'"external_bar"': ["multiplier"]}


def test_get_lineage_union(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT col FROM bar
           UNION
           SELECT col FROM baz;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT col FROM external_bar;"""
    )
    bar_sql_file = models_dir / "baz.sql"
    bar_sql_file.write_text(
        """MODEL (name baz);
           SELECT col FROM external_baz;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"], '"baz"': ["col"]}


def test_get_lineage_union_downstream(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT col FROM bar;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT col FROM baz
           UNION
           SELECT col FROM qwe;"""
    )
    bar_sql_file = models_dir / "baz.sql"
    bar_sql_file.write_text(
        """MODEL (name baz);
           SELECT col FROM external_baz;"""
    )
    bar_sql_file = models_dir / "qwe.sql"
    bar_sql_file.write_text(
        """MODEL (name qwe);
           SELECT col FROM external_qwe;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"], '"qwe"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_baz"': ["col"]}
    assert response_json['"qwe"']["col"]["models"] == {'"external_qwe"': ["col"]}


def test_get_lineage_cte_union(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           WITH my_cte AS (
               SELECT col FROM bar
               UNION
               SELECT col FROM baz
           )
           SELECT col FROM my_cte;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT col FROM external_bar;"""
    )
    bar_sql_file = models_dir / "baz.sql"
    bar_sql_file.write_text(
        """MODEL (name baz);
           SELECT col FROM external_baz;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {"my_cte": ["col"]}
    assert response_json["my_cte"]["col"]["models"] == {'"bar"': ["col"], '"baz"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"external_bar"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_baz"': ["col"]}


def test_get_lineage_cte_union_downstream(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT col FROM bar;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           WITH my_cte AS (
               SELECT col FROM baz
               UNION
               SELECT col FROM qwe
           )
           SELECT col FROM my_cte;"""
    )
    baz_sql_file = models_dir / "baz.sql"
    baz_sql_file.write_text(
        """MODEL (name baz);
           SELECT col FROM external_baz;"""
    )
    baz_sql_file = models_dir / "qwe.sql"
    baz_sql_file.write_text(
        """MODEL (name qwe);
           SELECT col FROM external_qwe;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {"my_cte": ["col"]}
    assert response_json["my_cte"]["col"]["models"] == {'"baz"': ["col"], '"qwe"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_baz"': ["col"]}
    assert response_json['"qwe"']["col"]["models"] == {'"external_qwe"': ["col"]}


def test_get_lineage_cte_downstream_union_downstream(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT col FROM bar;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           WITH my_cte AS (
               SELECT * FROM baz
           )
           SELECT col FROM my_cte;"""
    )
    baz_sql_file = models_dir / "baz.sql"
    baz_sql_file.write_text(
        """MODEL (name baz);
           SELECT col FROM external_table1
           UNION
           SELECT col FROM external_table2;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {"my_cte": ["col"]}
    assert response_json["my_cte"]["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {
        '"external_table1"': ["col"],
        '"external_table2"': ["col"],
    }


def test_get_lineage_nested_cte_union_downstream(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           WITH my_cte2 AS (
               SELECT * FROM bar
           ), my_cte1 AS (
               SELECT col FROM my_cte2
               UNION
               SELECT col FROM external_table1
           )
           SELECT col FROM my_cte1;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT col FROM external_table2
           UNION
           SELECT col FROM external_table3
           ;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {"my_cte1": ["col"]}
    assert response_json["my_cte1"]["col"]["models"] == {
        "my_cte2": ["col"],
        '"external_table1"': ["col"],
    }
    assert response_json["my_cte2"]["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {
        '"external_table2"': ["col"],
        '"external_table3"': ["col"],
    }
