from __future__ import annotations

import datetime
import typing as t
from pathlib import Path

import pytest

from sqlmesh.cli.example_project import init_example_project
from sqlmesh.core import constants as c
from sqlmesh.core.config import Config, DuckDBConnectionConfig, ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlmesh.core.test.definition import SqlModelTest
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.yaml import load as load_yaml

if t.TYPE_CHECKING:
    from unittest import TestResult

pytestmark = pytest.mark.slow

SUSHI_FOO_META = "MODEL (name sushi.foo, kind FULL)"


def _create_test(
    body: t.Dict[str, t.Any],
    test_name: str,
    model: SqlModel,
    context: Context,
) -> SqlModelTest:
    return SqlModelTest(
        body=body[test_name],
        test_name=test_name,
        model=model,
        models=context._models,
        engine_adapter=context._test_engine_adapter,
        dialect=context.config.dialect,
        path=None,
        default_catalog=context.default_catalog,
    )


def _create_model(
    query: str,
    meta: str = SUSHI_FOO_META,
    dialect: t.Optional[str] = None,
    default_catalog: t.Optional[str] = None,
) -> SqlModel:
    parsed_definition = parse(f"{meta};{query}", default_dialect=dialect)
    return t.cast(
        SqlModel,
        load_sql_based_model(parsed_definition, dialect=dialect, default_catalog=default_catalog),
    )


def _check_successful_or_raise(
    result: t.Optional[TestResult], expected_msg: t.Optional[str] = None
) -> None:
    assert result is not None
    if not result.wasSuccessful():
        error_or_failure_traceback = (result.errors or result.failures)[0][1]
        if expected_msg:
            assert expected_msg in error_or_failure_traceback
        else:
            raise AssertionError(error_or_failure_traceback)


@pytest.fixture
def full_model_without_ctes(request) -> SqlModel:
    return _create_model(
        "SELECT id, value, ds FROM raw",
        dialect=getattr(request, "param", None),
        default_catalog="memory",
    )


@pytest.fixture
def full_model_with_single_cte(request) -> SqlModel:
    return _create_model(
        "WITH source AS (SELECT id FROM raw) SELECT id FROM source",
        dialect=getattr(request, "param", None),
        default_catalog="memory",
    )


@pytest.fixture
def full_model_with_two_ctes(request) -> SqlModel:
    return _create_model(
        """
        WITH source AS (
            SELECT id FROM raw
        ),
        renamed AS (
            SELECT id AS fid FROM source
        )
        SELECT fid FROM renamed;
        """,
        dialect=getattr(request, "param", None),
        default_catalog="memory",
    )


def test_ctes(sushi_context: Context, full_model_with_two_ctes: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_with_two_ctes))
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
  outputs:
    ctes:
      source:
        - id: 1
      renamed:
        - fid: 1
    query:
      - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_ctes_only(sushi_context: Context, full_model_with_two_ctes: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_with_two_ctes))
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
  outputs:
    ctes:
      source:
        - id: 1
      renamed:
        - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_query_only(sushi_context: Context, full_model_with_two_ctes: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_with_two_ctes))
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
  outputs:
    query:
      - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_with_rows(sushi_context: Context, full_model_with_single_cte: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_with_single_cte))
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      rows:
        - id: 1
  outputs:
    ctes:
      source:
        rows:
          - id: 1
    query:
      rows:
        - id: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_without_rows(sushi_context: Context, full_model_with_single_cte: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_with_single_cte))
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
  outputs:
    ctes:
      source:
        - id: 1
    query:
      - id: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_column_order(sushi_context: Context, full_model_without_ctes: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_without_ctes))
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
        value: 2
        ds: 3
  outputs:
    query:
      - id: 1
        ds: 3
        value: 2
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_row_order(sushi_context: Context, full_model_without_ctes: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_without_ctes))

    # input and output rows are in different orders
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
        value: 2
        ds: 3
      - id: 2
        value: 3
        ds: 4
  outputs:
    query:
      - id: 2
        value: 3
        ds: 4
      - id: 1
        value: 2
        ds: 3
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )

    # model query without ORDER BY should pass unit test
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)

    # model query with ORDER BY should fail unit test
    full_model_without_ctes_dict = full_model_without_ctes.dict()
    full_model_without_ctes_dict["query"] = full_model_without_ctes.query.order_by("id")  # type: ignore
    full_model_without_ctes_orderby = SqlModel(**full_model_without_ctes_dict)

    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_without_ctes_orderby))
    result = _create_test(body, "test_foo", model, sushi_context).run()

    expected_failure_msg = """AssertionError: Data differs (exp: expected, act: actual)

   id     value      ds    
  exp act   exp act exp act
0   2   1     3   2   4   3
1   1   2     2   3   3   4
"""
    _check_successful_or_raise(result, expected_msg=expected_failure_msg)


def test_partial_data(sushi_context: Context) -> None:
    model = _create_model(
        "WITH source AS (SELECT id, name FROM sushi.waiter_names) SELECT id, name, 'nan' as str FROM source",
        default_catalog=sushi_context.default_catalog,
    )
    model = t.cast(SqlModel, sushi_context.upsert_model(model))

    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    sushi.waiter_names:
      - id: 1
      - id: 2
        name: null
      - id: 3
        name: 'bob'
  outputs:
    ctes:
      source:
        - id: 1
        - id: 2
          name: null
        - id: 3
          name: 'bob'
    query:
      - id: 1
        str: nan
      - id: 2
        str: nan
      - id: 3
        name: 'bob'
        str: nan
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_partial_output_columns() -> None:
    result = _create_test(
        body=load_yaml(
            """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - a: 1
        b: 2
        c: 3
        d: 4
      - a: 5
        b: 6
        c: 7
  outputs:
    ctes:
      t:
        partial: true
        rows:
          - c: 3
          - c: 7
    query:
      partial: true
      rows:
        - a: 1
          b: 2
        - a: 5
          b: 6
            """
        ),
        test_name="test_foo",
        model=_create_model("WITH t AS (SELECT a, b, c, d FROM raw) SELECT a, b, c, d FROM t"),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
    ).run()

    _check_successful_or_raise(result)


def test_partial_data_column_order(sushi_context: Context) -> None:
    model = _create_model(
        "SELECT id, name, price, event_date FROM sushi.items",
        default_catalog=sushi_context.default_catalog,
    )
    model = t.cast(SqlModel, sushi_context.upsert_model(model))

    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    sushi.items:
      - id: 1234
        event_date: 2020-01-01
      - id: 9876
        name: hello
        event_date: 2020-01-02
  outputs:
    query:
      - id: 1234
        event_date: 2020-01-01
      - id: 9876
        name: hello
        event_date: 2020-01-02
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_partial_data_missing_schemas(sushi_context: Context) -> None:
    model = _create_model("SELECT * FROM unknown")
    model = t.cast(SqlModel, sushi_context.upsert_model(model))

    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    unknown:
      - a: 1
        b: bla
      - b: baz
  outputs:
    query:
      - a: 1
        b: bla
      - b: baz
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)

    model = _create_model(
        "SELECT *, DATE_TRUNC('month', date)::DATE AS month, NULL::DATE AS null_date, FROM unknown"
    )
    model = t.cast(SqlModel, sushi_context.upsert_model(model))

    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    unknown:
      - id: 1234
        date: 2023-01-12
      - id: 9876
        date: 2023-02-10
  outputs:
    query:
      - id: 1234
        date: 2023-01-12
        month: 2023-01-01
        null_date:
      - id: 9876
        date: 2023-02-10
        month: 2023-02-01
        null_date:
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


def test_missing_column_failure(sushi_context: Context, full_model_without_ctes: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_without_ctes))
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  description: sushi.foo's output has a missing column (fails intentionally)
  inputs:
    raw:
      - id: 1
        value: 2
        ds: 3
  outputs:
    query:
      - id: 1
        value: null
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()

    expected_failure_msg = """AssertionError: Data differs (exp: expected, act: actual)

  value      ds    
    exp act exp act
0   NaN   2 NaN   3


Test description: sushi.foo's output has a missing column (fails intentionally)
"""
    _check_successful_or_raise(result, expected_msg=expected_failure_msg)


def test_unknown_column_error() -> None:
    result = _create_test(
        body=load_yaml(
            """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
        value: 2
  outputs:
    query:
      - foo: 1
                """
        ),
        test_name="test_foo",
        model=_create_model("SELECT id, value FROM raw"),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
    ).run()

    expected_error_msg = """sqlmesh.core.test.definition.TestError: Detected unknown column(s)

Expected column(s): id, value
Unknown column(s): foo
"""
    _check_successful_or_raise(result, expected_msg=expected_error_msg)


def test_empty_rows(sushi_context: Context) -> None:
    model = _create_model(
        "SELECT id FROM sushi.items", default_catalog=sushi_context.default_catalog
    )
    model = t.cast(SqlModel, sushi_context.upsert_model(model))

    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    sushi.items: []
  outputs:
    query: []
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    _check_successful_or_raise(result)


@pytest.mark.parametrize("full_model_without_ctes", ["snowflake"], indirect=True)
def test_normalization(full_model_without_ctes: SqlModel) -> None:
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
  outputs:
    ctes:
      source:
        rows:
          - id: 1
      renamed:
        - fid: 1
    query:
      - fid: 1
  vars:
    start: 2022-01-01
    end: 2022-01-01
        """
    )

    context = Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="snowflake")))
    normalized_body = _create_test(body, "test_foo", full_model_without_ctes, context).body

    expected_body = {
        "model": '"MEMORY"."SUSHI"."FOO"',
        "inputs": {'"RAW"': {"rows": [{"ID": 1}]}},
        "outputs": {
            "ctes": {'"SOURCE"': {"rows": [{"ID": 1}]}, '"RENAMED"': {"rows": [{"FID": 1}]}},
            "query": {"rows": [{"FID": 1}]},
        },
        "vars": {"start": datetime.date(2022, 1, 1), "end": datetime.date(2022, 1, 1)},
    }

    assert expected_body == normalized_body


def test_test_generation(tmp_path: Path) -> None:
    init_example_project(tmp_path, dialect="duckdb")

    config = Config(
        default_connection=DuckDBConnectionConfig(),
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )

    context = Context(paths=tmp_path, config=config)
    context.plan(auto_apply=True)

    input_queries = {
        "sqlmesh_example.incremental_model": f"SELECT * FROM sqlmesh_example.incremental_model LIMIT 3"
    }

    with pytest.raises(ConfigError) as ex:
        context.create_test("sqlmesh_example.full_model", input_queries=input_queries)

    assert (
        "tests/test_full_model.yaml' already exists, "
        "make sure to set --overwrite if it can be safely overwritten."
    ) in str(ex.value)

    test = load_yaml(context.path / c.TESTS / "test_full_model.yaml")

    assert len(test) == 1
    assert "test_example_full_model" in test
    assert "vars" not in test["test_example_full_model"]

    context.create_test(
        "sqlmesh_example.full_model",
        input_queries=input_queries,
        overwrite=True,
        variables={"start": "2020-01-01", "end": "2024-01-01"},
    )

    test = load_yaml(context.path / c.TESTS / "test_full_model.yaml")

    assert len(test) == 1
    assert "test_full_model" in test
    assert "vars" in test["test_full_model"]
    assert test["test_full_model"]["vars"] == {"start": "2020-01-01", "end": "2024-01-01"}

    result = context.test()
    _check_successful_or_raise(result)

    context.create_test(
        "sqlmesh_example.full_model", input_queries=input_queries, name="new_name", path="foo/bar"
    )

    test = load_yaml(context.path / c.TESTS / "foo/bar.yaml")
    assert len(test) == 1
    assert "new_name" in test


def test_source_func() -> None:
    result = _create_test(
        body=load_yaml(
            """
test_foo:
  model: xyz
  outputs:
    query:
      - month: 2023-01-01
      - month: 2023-02-01
      - month: 2023-03-01
            """
        ),
        test_name="test_foo",
        model=_create_model(
            """
            SELECT range::DATE AS month
            FROM RANGE(DATE '2023-01-01', DATE '2023-04-01', INTERVAL 1 MONTH) AS r
            """
        ),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
    ).run()

    _check_successful_or_raise(result)


def test_nested_data_types() -> None:
    result = _create_test(
        body=load_yaml(
            """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - value: [1, 2, 3]
      - value:
        - 2
        - 3
      - value: [0, 4, 1]
  outputs:
    query:
      - value: [0, 4, 1]
      - value: [1, 2, 3]
      - value: [2, 3]
            """
        ),
        test_name="test_foo",
        model=_create_model("SELECT value FROM raw"),
        context=Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))),
    ).run()

    _check_successful_or_raise(result)
