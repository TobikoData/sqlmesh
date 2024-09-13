from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sqlmesh.core.context import Context

pytestmark = pytest.mark.web


@pytest.fixture
def client(web_app: FastAPI) -> TestClient:
    return TestClient(web_app)


def test_get_lineage(client: TestClient, web_sushi_context: Context) -> None:
    response = client.get("/api/lineage/sushi.waiters/event_date")

    assert response.status_code == 200
    assert response.json() == {
        '"memory"."sushi"."waiters"': {
            "event_date": {
                "source": """SELECT DISTINCT
  CAST("o"."event_date" AS DATE) AS "event_date"
FROM "memory"."sushi"."orders" AS "o"
WHERE
  "o"."event_date" <= CAST('1970-01-01' AS DATE)
  AND "o"."event_date" >= CAST('1970-01-01' AS DATE)""",
                "expression": 'CAST("o"."event_date" AS DATE) AS "event_date"',
                "models": {'"memory"."sushi"."orders"': ["event_date"]},
            }
        },
        '"memory"."sushi"."orders"': {
            "event_date": {
                "source": 'SELECT\n  CAST(NULL AS DATE) AS "event_date"\nFROM (VALUES\n  (1)) AS "t"("dummy")',
                "expression": 'CAST(NULL AS DATE) AS "event_date"',
                "models": {},
            }
        },
    }

    response = client.get("/api/lineage/sushi.customers/customer_id")
    assert response.status_code == 200
    assert response.json() == {
        '"memory"."sushi"."customers"': {
            "customer_id": {
                "expression": 'CAST("o"."customer_id" AS INT) AS "customer_id" /* this comment should not be registered */',
                "models": {'"memory"."sushi"."orders"': ["customer_id"]},
                "source": '''WITH "current_marketing" AS (
  SELECT
    "marketing"."customer_id" AS "customer_id",
    "marketing"."status" AS "status"
  FROM "memory"."sushi"."marketing" AS "marketing"
  WHERE
    "marketing"."valid_to" IS NULL
)
SELECT DISTINCT
  CAST("o"."customer_id" AS INT) AS "customer_id" /* this comment should not be registered */
FROM "memory"."sushi"."orders" AS "o"
LEFT JOIN "current_marketing" AS "m"
  ON "m"."customer_id" = "o"."customer_id"
LEFT JOIN "memory"."raw"."demographics" AS "d"
  ON "d"."customer_id" = "o"."customer_id"''',
            }
        },
        '"memory"."sushi"."orders"': {
            "customer_id": {
                "expression": 'CAST(NULL AS INT) AS "customer_id"',
                "models": {},
                "source": """SELECT
  CAST(NULL AS INT) AS "customer_id"
FROM (VALUES
  (1)) AS "t"("dummy")""",
            }
        },
    }


def test_get_lineage_managed_columns(client: TestClient, web_sushi_context: Context) -> None:
    # Get lineage of managed column
    response = client.get("/api/lineage/sushi.marketing/valid_from")
    assert response.status_code == 200
    assert response.json() == {
        '"memory"."sushi"."marketing"': {
            "valid_from": {
                "source": '''SELECT
  CAST(NULL AS TIMESTAMP) AS "valid_from"
FROM "memory"."sushi"."raw_marketing" AS "raw_marketing"''',
                "expression": 'CAST(NULL AS TIMESTAMP) AS "valid_from"',
                "models": {},
            }
        }
    }


def test_get_lineage_single_model(client: TestClient, project_context: Context) -> None:
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


def test_get_lineage_external_model(client: TestClient, project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text("MODEL (name foo); SELECT col FROM bar;")
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text("MODEL (name bar); SELECT col FROM baz;")
    baz_sql_file = models_dir / "baz.sql"
    baz_sql_file.write_text("MODEL (name baz); SELECT col FROM external_table;")
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_cte(client: TestClient, project_context: Context) -> None:
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
    assert response_json['"foo"']["col"]["models"] == {'"foo": my_cte': ["col"]}
    assert response_json['"foo": my_cte']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_cte_downstream(client: TestClient, project_context: Context) -> None:
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
    assert response_json['"bar"']["col"]["models"] == {'"bar": my_cte': ["col"]}
    assert response_json['"bar": my_cte']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_join(client: TestClient, project_context: Context) -> None:
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


def test_get_lineage_multiple_columns(client: TestClient, project_context: Context) -> None:
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


def test_get_lineage_union(client: TestClient, project_context: Context) -> None:
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

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"], '"baz"': ["col"]}


def test_get_lineage_union_downstream(client: TestClient, project_context: Context) -> None:
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

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"], '"qwe"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_baz"': ["col"]}
    assert response_json['"qwe"']["col"]["models"] == {'"external_qwe"': ["col"]}


def test_get_lineage_cte_union(client: TestClient, project_context: Context) -> None:
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
    assert response_json['"foo"']["col"]["models"] == {'"foo": my_cte': ["col"]}
    assert response_json['"foo": my_cte']["col"]["models"] == {'"bar"': ["col"], '"baz"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"external_bar"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_baz"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"], '"baz"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"external_bar"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_baz"': ["col"]}


def test_get_lineage_cte_union_downstream(client: TestClient, project_context: Context) -> None:
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
    assert response_json['"bar"']["col"]["models"] == {'"bar": my_cte': ["col"]}
    assert response_json['"bar": my_cte']["col"]["models"] == {'"baz"': ["col"], '"qwe"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_baz"': ["col"]}
    assert response_json['"qwe"']["col"]["models"] == {'"external_qwe"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"], '"qwe"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_baz"': ["col"]}
    assert response_json['"qwe"']["col"]["models"] == {'"external_qwe"': ["col"]}


def test_get_lineage_cte_downstream_union_downstream(
    client: TestClient, project_context: Context
) -> None:
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
    assert response_json['"bar"']["col"]["models"] == {'"bar": my_cte': ["col"]}
    assert response_json['"bar": my_cte']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {
        '"external_table1"': ["col"],
        '"external_table2"': ["col"],
    }

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {
        '"external_table1"': ["col"],
        '"external_table2"': ["col"],
    }


def test_get_lineage_nested_cte_union_downstream(
    client: TestClient, project_context: Context
) -> None:
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
    assert response_json['"foo"']["col"]["models"] == {'"foo": my_cte1': ["col"]}
    assert response_json['"foo": my_cte1']["col"]["models"] == {
        '"foo": my_cte2': ["col"],
        '"external_table1"': ["col"],
    }
    assert response_json['"foo": my_cte2']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {
        '"external_table2"': ["col"],
        '"external_table3"': ["col"],
    }

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {
        '"bar"': ["col"],
        '"external_table1"': ["col"],
    }
    assert response_json['"bar"']["col"]["models"] == {
        '"external_table2"': ["col"],
        '"external_table3"': ["col"],
    }


def test_get_lineage_subquery(client: TestClient, project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT col FROM (SELECT col FROM bar) my_dt;"""
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
    assert response_json['"foo"']["col"]["models"] == {'"foo": my_dt': ["col"]}
    assert response_json['"foo": my_dt']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_cte_name_collision(client: TestClient, project_context: Context) -> None:
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
    assert response_json['"foo"']["col"]["models"] == {'"foo": my_cte': ["col"]}
    assert response_json['"foo": my_cte']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"bar": my_cte': ["col"]}
    assert response_json['"bar": my_cte']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_derived_table_alias_collision(
    client: TestClient, project_context: Context
) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           SELECT col FROM (SELECT col FROM bar) my_dt;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT col FROM (SELECT col FROM baz) my_dt;"""
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
    assert response_json['"foo"']["col"]["models"] == {'"foo": my_dt': ["col"]}
    assert response_json['"foo": my_dt']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"bar": my_dt': ["col"]}
    assert response_json['"bar": my_dt']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"baz"': ["col"]}
    assert response_json['"baz"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_constants(client: TestClient, project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           WITH my_cte AS (
               SELECT col FROM bar
               UNION
               SELECT NULL::TIMESTAMP as col FROM bar
               UNION
               SELECT 1 as col FROM external_table
           )
           SELECT col FROM my_cte;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT col FROM external_table;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"foo": my_cte': ["col"]}
    assert response_json['"foo": my_cte']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"external_table"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"external_table"': ["col"]}


def test_get_lineage_quoted_columns(client: TestClient, project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text(
        """MODEL (name foo);
           WITH my_cte AS (
               SELECT col as "@col" FROM bar
               UNION
               SELECT NULL::TIMESTAMP as "@col" FROM bar
               UNION
               SELECT 1 as "@col" FROM external_table
           )
           SELECT "@col" FROM my_cte;"""
    )
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text(
        """MODEL (name bar);
           SELECT col FROM external_table;"""
    )
    project_context.load()

    response = client.get("/api/lineage/foo/@col")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["@col"]["models"] == {'"foo": my_cte': ["@col"]}
    assert response_json['"foo": my_cte']["@col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"external_table"': ["col"]}

    # Models only
    response = client.get("/api/lineage/foo/@col?models_only=1")
    assert response.status_code == 200, response.json()
    response_json = response.json()
    assert response_json['"foo"']["@col"]["models"] == {'"bar"': ["col"]}
    assert response_json['"bar"']["col"]["models"] == {'"external_table"': ["col"]}
