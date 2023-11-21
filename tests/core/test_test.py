from __future__ import annotations

import datetime
import typing as t

import pytest

from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlmesh.core.test.definition import SqlModelTest
from sqlmesh.utils.yaml import load as load_yaml


@pytest.fixture
def full_model_without_ctes(request) -> SqlModel:
    dialect = getattr(request, "param", None)
    return t.cast(
        SqlModel,
        load_sql_based_model(
            parse(
                """
                MODEL (
                    name sushi.foo,
                    kind FULL,
                );

                SELECT id, value, ds FROM raw;
                """,
                default_dialect=dialect,
            ),
            dialect=dialect,
        ),
    )


@pytest.fixture
def full_model_with_single_cte(request) -> SqlModel:
    dialect = getattr(request, "param", None)
    return t.cast(
        SqlModel,
        load_sql_based_model(
            parse(
                """
                MODEL (
                    name sushi.foo,
                    kind FULL,
                );

                WITH source AS (
                    SELECT id FROM raw
                )
                SELECT id FROM source;
                """,
                default_dialect=dialect,
            ),
            dialect=dialect,
        ),
    )


@pytest.fixture
def full_model_with_two_ctes(request) -> SqlModel:
    dialect = getattr(request, "param", None)
    return t.cast(
        SqlModel,
        load_sql_based_model(
            parse(
                """
                MODEL (
                    name sushi.foo,
                    kind FULL,
                );

                WITH source AS (
                    SELECT id FROM raw
                ),
                renamed AS (
                    SELECT id as fid FROM source
                )
                SELECT fid FROM renamed;
                """,
                default_dialect=dialect,
            ),
            dialect=dialect,
        ),
    )


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
    assert result and result.wasSuccessful()


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
    assert result and result.wasSuccessful()


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
    assert result and result.wasSuccessful()


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
    assert result and result.wasSuccessful()


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
    assert result and result.wasSuccessful()


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
    assert result and result.wasSuccessful()


def test_nan(sushi_context: Context, full_model_without_ctes: SqlModel) -> None:
    model = t.cast(SqlModel, sushi_context.upsert_model(full_model_without_ctes))
    body = load_yaml(
        """
test_foo:
  model: sushi.foo
  inputs:
    raw:
      - id: 1
        value: nan
        ds: 3
  outputs:
    query:
      - id: 1
        value: null
        ds: 3
  vars:
    start: 2022-01-01
    end: 2022-01-01
            """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    assert result and result.wasSuccessful()


def test_partial_inputs(sushi_context: Context) -> None:
    model = t.cast(
        SqlModel,
        sushi_context.upsert_model(
            load_sql_based_model(
                parse(
                    """
                    MODEL (
                        name sushi.foo,
                        kind FULL
                    );

                    WITH source AS (
                        SELECT id, name FROM sushi.waiter_names
                    )
                    SELECT id, name FROM source;
                    """,
                ),
            ),
        ),
    )

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
      - id: 2
      - id: 3
        name: 'bob'
        """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    assert result and result.wasSuccessful()


def test_missing_column_failure(sushi_context: Context, full_model_without_ctes: SqlModel) -> None:
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
        value: null
            """
    )
    result = _create_test(body, "test_foo", model, sushi_context).run()
    assert result and not result.wasSuccessful()

    expected_msg = "AssertionError: Data differs (exp: expected, act: actual)\n\n  value        ds    \n    exp act   exp act\n0  None   2  None   3\n"
    assert expected_msg in result.failures[0][1]


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
        "model": "SUSHI.FOO",
        "inputs": {"RAW": [{"ID": 1}]},
        "outputs": {
            "ctes": {"SOURCE": [{"ID": 1}], "RENAMED": [{"FID": 1}]},
            "query": [{"FID": 1}],
        },
        "vars": {"start": datetime.date(2022, 1, 1), "end": datetime.date(2022, 1, 1)},
    }

    assert expected_body == normalized_body
